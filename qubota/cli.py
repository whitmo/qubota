from gevent.monkey import patch_all
patch_all()

from . import job 
from . import utils
from .wmm import parts_to_mm
from boto.sqs.jsonmessage import JSONMessage
#from botox import aws
from botox.utils import msg, puts
from cliff.app import App
from cliff.command import Command
from cliff.commandmanager import CommandManager
from cliff.lister import Lister
from functools import partial
from path import path
from pprint import pformat
from stuf import stuf
import gevent
import argparse
import boto
import logging
import pkg_resources
import sys
import yaml
import base64
import tempfile
import botox


class CLIApp(App):
    """
    command line interface
    """
    specifier = 'qubota.cli'
    version = pkg_resources.get_distribution('qubota').version
    prefix = 'qubota'
    pa_tmplt = utils.readf('postactivate.tmplt')
    log = logging.getLogger(__name__)

    def __init__(self):
        self._settings = None
        self._aws = None
        self._sqs = None
        self._sdb = None
        super(CLIApp, self).__init__(
            description=self.specifier,
            version=self.version,
            command_manager=CommandManager(self.specifier)
        )
        self.msg = partial(msg, 
                           printer=partial(puts, 
                                           stream=self.stdout))

    @property
    def insts_up(self):
        return [x for x in self.insts \
                    if x.state not in set(('stopped', 'terminated'))]

    def postactivate_tmplt(self):
        # we'll reuse the env vars for now, but...
        # should parameterize
        vals = dict((key, getattr(self.aws, key)) for key in botox.aws.PARAMETERS.keys()\
                        + ['secret_access_key', 'access_key_id'])
        vals['editor'] = 'emacs' #@@ make param
        out = self.pa_tmplt.format(**vals)
        return out

    def initialize_app(self, argv):
        logging.getLogger('boto').setLevel(logging.CRITICAL)

    def msgs(self, qname=prefix, num=1, vistime=None):
        """
        Return messages in job queue
        """
        queue = self.queue(qname)
        queue.set_message_class(JSONMessage)
        return queue.get_messages(num, vistime)

    def queue(self, name=prefix): 
        mq = self.sqs.lookup(name)
        if mq is None:
            raise ValueError('Queue missing: %s' %name)
        return mq

    def domain(self, name=prefix):
        dbdom = self.sdb.lookup(name)
        if dbdom is None:
            raise ValueError("Domain missing: %s" %name)
        return dbdom

    @property
    def aws(self):
        if self._aws is None:
            #@ parameterize
            # we assume use of environ vars
            self._aws = botox.aws.AWS()
        return self._aws

    @property
    def sqs(self):
        """
        A connection to SQS
        """
        if self._sqs is None:
            self._sqs = boto.connect_sqs(self.aws.access_key_id, 
                                         self.aws.secret_access_key)
        return self._sqs
    
    @property
    def sdb(self):
        """
        A connection to SDB
        """
        if self._sdb is None:
            self._sdb = boto.connect_sdb(self.aws.access_key_id, 
                                                self.aws.secret_access_key)
        return self._sdb

    @property
    def insts(self):
        return (x for x in self.aws.instances if x.name.startswith(self.prefix))


class QCommand(Command):
    aws = utils.app_attr('aws')
    sqs = utils.app_attr('sqs')
    sdb = utils.app_attr('sdb')
    msg = utils.app_attr('msg')
    prefix = CLIApp.prefix


class QUp(QCommand):
    """
    Brings up or builds a queue system

    - launch and/or bootstrap precise cloudinit instances
    - set up sdb domain
    - set up SQS queues
              
    Some day this should all be done via CloudFormation, bfn, botox it.
    """
    tempdir = path(tempfile.mkdtemp())
    upstart_tmp = tempdir / 'drain.conf'

    clc_tmp = tempdir / 'cloud-config.yml'
    pa_tmp = tempdir / '0_place-postactivate.sh'

    mkvenv = utils.readf('mkvenv.sh')
    mkvenv_tmp = tempdir / '1_mkvenv.sh'

    drain_start = utils.readf('drain_start.sh')
    drain_start_tmp = tempdir / '2_drain_start.sh'

    cl = path(__file__).parent / 'cloud-init.yml'
    ami = utils.readf('ami.txt')
    upstart = utils.readf('upstart.conf')
    filewriter = utils.readf('write-file.sh')
    b64enc = staticmethod(base64.encodestring)
    parts_to_mm = staticmethod(parts_to_mm)

    def get_parser(self, prog_name):
        parser = super(QUp, self).get_parser(prog_name)
        parser.add_argument('--queue', '-q', default=CLIApp.prefix, help="Queue name")
        parser.add_argument('--numworkers', '-n', default=1, type=int, help="How many workers")
        parser.add_argument('--domain', '-d', default=CLIApp.prefix, help="Domain name")
        parser.add_argument('--visibility-timeout', '-t', default=60, help="Default visibility timeout in seconds")
        return parser

    def cloud_config(self):
        with open(self.cl) as stream:
            ci_data = yaml.load(stream)
        ci_data['write_files'][0]['content'] = self.b64enc(self.app.postactivate_tmplt())
        return ci_data

    def make_user_data(self):
        rpa = path('/home/ec2-user/app/postactivate')
        pa = self.app.postactivate_tmplt()
        paus = self.filewriter.format(parent=rpa.parent, filepath=rpa, content=pa)

        self.mkvenv_tmp.write_text(self.mkvenv)
        self.drain_start_tmp.write_text(self.drain_start)

        ci = "#cloud-config\n" + yaml.dump(self.cloud_config())
        self.clc_tmp.write_text(ci)
        self.pa_tmp.write_text(paus)
        self.upstart_tmp.write_text(self.upstart)
        mime = self.parts_to_mm([self.clc_tmp, 
                                 (self.pa_tmp, 'text/x-shellscript'),
                                 (self.mkvenv_tmp, 'text/x-shellscript'),
                                 (self.drain_start_tmp, 'text/x-shellscript'),
                                 (self.upstart_tmp, 'text/upstart-job')])

        return mime.as_string()

    def up_node(self, name):
        userdata = self.make_user_data()
        inst = self.aws.create(name, user_data=userdata, ami=self.ami)
        self.app.stdout.write("{} @ {}\n".format(name, inst.public_dns_name))
        return inst

    def take_action(self, pargs):
        """
        check, ammend, return
        """

        if not self.sqs.lookup(pargs.queue):
            with self.app.msg("Adding queue: %s" %pargs.queue):
                mq = self.sqs.create_queue(pargs.queue)

        if not self.sdb.lookup(pargs.domain):
            with self.app.msg("Adding domain: %s" %pargs.domain):
                self.sdb.create_domain(pargs.domain)

        up = len(self.app.insts_up) 
        if not up >= pargs.numworkers:
            # up with drainer / web
            with self.msg("Bringing up workers"):
                self.app.stdout.write('\n')
                names = ("{}:{}".format(self.app.prefix, num) \
                             for num in range(pargs.numworkers - up))
                gevent.joinall([gevent.spawn(self.up_node, name) for name in names])
        

class QDown(Command):
    """
    Brings the queue system down

    - shutdown ec2 instances
    - other ??
    """
    def get_parser(self, prog_name):
        parser = super(QDown, self).get_parser(prog_name)
        parser.add_argument('--queue', '-q', default=CLIApp.prefix, 
                            help="Queue name")
        parser.add_argument('--domain', default=CLIApp.prefix, 
                            help="sdb domain")
        parser.add_argument('--delete-queue', '-m', action='store_true', default=False, 
                            help='Delete queue')
        parser.add_argument('--delete-sdb', '-d', action='store_true', default=False, 
                            help='Delete sdb domains')
        return parser

    def take_action(self, pargs):
        for inst in self.app.insts_up:
            with self.app.msg("Terminating %s" %inst.name):
                inst.terminate()

        if pargs.delete_queue:
            q = self.app.sqs.lookup(pargs.queue)
            if q is not None:
                with self.app.msg("Deleting queue %s" %pargs.queue):
                    self.app.sqs.delete_queue(q)

        if pargs.delete_sdb:
            dom = self.app.sdb.lookup(pargs.queue)
            if dom is not None:
                with self.app.msg("Deleting domain %s" %pargs.queue):
                    self.app.sdb.delete_domain(dom)


class EnqueueJob(Command):
    """
    Queue up a job
    """
    enqueue = staticmethod(job.Job.enqueue)

    def get_parser(self, prog_name):
        parser = super(EnqueueJob, self).get_parser(prog_name)
        parser.add_argument('path', 
                            help="Dotted name for loading job from python path")
        parser.add_argument('--queue', '-q', default=CLIApp.prefix, help="Queue name")
        parser.add_argument('--args', '-a', default='', help="Arguments")
        parser.add_argument('--howmany', '-n', default=1, type=int, help="Arguments")
        return parser

    def take_action(self, pargs):
        """
        enqueue a job
        """
        args, kwargs = None, None
        if pargs.args:
            args, kwargs = self.parse_job_args(pargs.args)

        for x in range(pargs.howmany):
            job = self.enqueue(self.app.queue(pargs.queue), 
                               self.app.domain(pargs.queue),
                               pargs.path, args, kwargs)
            self.app.stdout.write('%s:%s\n' %(pargs.queue, job.id))


class ShowJobs(Lister):
    """
    Show jobs recorded
    """
    def get_parser(self, prog_name):
        parser = super(ShowJobs, self).get_parser(prog_name)
        parser.add_argument('--queue', '-q', default=CLIApp.prefix, help="Queue name")
        parser.add_argument('--sql', type=str, help="SDB select statement",
                            default="select * from %(queue)s")
        return parser

    def take_action(self, pargs):
        res = self.app.domain(pargs.queue).select(pargs.sql % dict(queue=pargs.queue))
        return (('id', 'job'), ((x['id'], pformat(x)) for x in res))


class ShowMsgs(Lister):
    """
    show messages in queue
    """
    def get_parser(self, prog_name):
        parser = super(ShowMsgs, self).get_parser(prog_name)
        parser.add_argument('--queue', '-q', default=CLIApp.prefix, help="Queue name")
        parser.add_argument('--msgs', type=int, default=10, 
                            help="How many messages to pull off the queue")
        parser.add_argument('--wait', type=int, default=0.5, 
                            help="How long to wait for a message")
        parser.add_argument('--delete', action='store_true', default=False, 
                            help="How long to wait for a message")
        return parser

    def take_action(self, pargs):
        msgs = self.app.msgs(pargs.queue, pargs.msgs, 1)
        out = [(x.id, pformat(x.get_body())) for x in msgs]

        if pargs.delete:
            [x.delete() for x in msgs]

        return (('sqs id', 'msg'), out)


class Drain(Command):
    """
    Start a queue drain
    """
    res_py = staticmethod(utils.resolve)
    service = 'qubota.drain.Drain'

    def get_config(self, candidate):
        # turn it into a dict
        return stuf(candidate)

    def get_parser(self, prog_name):
        parser = super(Drain, self).get_parser(prog_name)
        parser.formater_class = argparse.ArgumentDefaultsHelpFormatter
        parser.add_argument("-c", "--config", type=self.get_config, default=stuf(), help="Where to find the settings for the Drain")
        parser.add_argument('--queue', '-q', default=CLIApp.prefix, help="Queue name")
        self.parser = parser
        return parser

    def take_action(self, pargs):
        config = pargs.config
        config.queue = self.app.queue(pargs.queue)
        config.domain = self.app.domain(pargs.queue)

        with utils.app(self.service, config) as app:
            app.serve_forever()


class NoiseMaker(QCommand):

    def take_action(self, pargs):
        from .service import Drone
        from .job import Job 
        from .zmqutils import Context

        qj = Job(path='qubota.tests.simple_job', kwargs=dict(howlong=2))
        job = Drone.spawn(qj)

        job.start()

        ctx = Context.instance()
        puller = ctx.pull(bind=Drone.drain)

        def callback():
            print(puller.recv_json())
        
        gevent.spawn(callback).join()

        job.join()
        

def main(argv=sys.argv[1:], app=CLIApp):
    return app().run(argv)







