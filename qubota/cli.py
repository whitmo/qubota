from . import job 
from . import utils
from .wmm import parts_to_mm
from boto.sqs.jsonmessage import JSONMessage
from botox import aws
from botox.utils import msg, puts
from cliff.app import App
from cliff.command import Command
from cliff.commandmanager import CommandManager
from cliff.lister import Lister
from functools import partial
from path import path
from pprint import pformat
from stuf import stuf
import argparse
import boto
import logging
import pkg_resources
import sys
import yaml
import base64
import tempfile


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

    def postactivate_tmplt(self):
        # we'll reuse the env vars for now, but...
        # should parameterize
        vals = dict((key, getattr(self.aws, key)) for key in aws.PARAMETERS.keys()\
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
            self._aws = aws.AWS()
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
    def qinsts(self):
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
    upstart_drain_tmp = tempdir / 'drain.conf'
    clc_tmp = tempdir / 'cloud-config.yml'
    pa_tmp = tempdir / 'postactivate'
    cl = path(__file__).parent / 'cloud-init.yml'
    ami = utils.readf('ami.txt')
    upstart_drain = utils.readf('upstart-drain.conf')
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
        ci = "#cloud-config\n" + yaml.dump(self.cloud_config())
        self.clc_tmp.write_text(ci)
        self.pa_tmp.write_text(paus)
        self.upstart_drain_tmp.write_text(self.upstart_drain.text())
        mime = self.parts_to_mm([self.clc_tmp, 
                                 (self.pa_tmp, 'text/x-shellscript'),
                                 (self.up_tmp, 'text/upstart-job')])
        return mime.as_string()

    def up_node(self, name):
        userdata = self.make_user_data()
        inst = self.aws.create(name, user_data=userdata, ami=self.ami)
        return inst

    def take_action(self, pargs):
        """
        check, ammend, return
        """
        if not len([x for x in self.app.qinsts \
                        if x.state not in set(('stopped', 'terminated'))]):
            # up with drainer / web
            with self.msg("Bringing up workers"):
                #make concurrent
                for num in range(pargs.numworkers):
                    name = "{}:{}".format(self.app.prefix, num)
                    inst = self.up_node(name)
                    self.app.stdout.write(inst.public_dns_name + '\n')

        if not self.sqs.lookup(pargs.queue):
            with self.app.msg("Adding queue: %s" %pargs.queue):
                mq = self.sqs.create_queue(pargs.queue)

        if not self.sdb.lookup(pargs.domain):
            with self.app.msg("Adding domain: %s" %pargs.domain):
                self.sdb.create_domain(pargs.domain)

        return [x for x in self.app.qinsts]
        

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
        for inst in self.app.qinsts:
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
        return parser

    def take_action(self, pargs):
        """
        enqueue a job
        """
        args, kwargs = None, None
        if pargs.args:
            args, kwargs = self.parse_job_args(pargs.args)

        job = self.enqueue(self.app.domain(pargs.queue), 
                           self.app.queue(pargs.queue), 
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


class Run(Command):
    """
    Run a ginkgo based service
    """
    runit = staticmethod(utils.runner)

    def get_parser(self, prog_name):
        parser = super(Run, self).get_parser(prog_name)

        parser.add_argument("-s", "--config-help", 
                            action="store_true", default=None, 
                            help="""show service's config options and exit""")

        parser.add_argument('--queue', '-q', default=CLIApp.prefix, help="Queue name")
        parser.add_argument("-d", "--daemonize", action="store_true", 
                            help="daemonize the service process")

        parser.add_argument("target", nargs='?', help="""
        service class path to run (modulename.ServiceClass) or
        configuration file path to use (/path/to/config.py)
        """.strip())
        self.parser = parser
        return parser

    def take_action(self, pargs):
        """
        launch daemon
        """
        self.runit(pargs.target, 
                   pargs.config_help, 
                   self.app.parser.error, 
                   self.app.stdout,
                   pargs.daemonize,
                   partial(self.app.parser.print_usage, self.app.stdout))


class Ctl(Command):
    """
    Control a ginkgo based service
    """
    ctl = staticmethod(utils.control)
    def get_parser(self, prog_name):
        parser = super(Ctl, self).get_parser(prog_name)
        parser.add_argument("-p", "--pid", help="""
        pid or pidfile to use instead of target
        """.strip())
        parser.add_argument("target", nargs='?', help="""
        service class path to use (modulename.ServiceClass) or
        configuration file path to use (/path/to/config.py)
        """.strip())
        parser.add_argument("action",
                            choices="start stop restart reload status log logtail".split())
        return parser
    
    def take_action(self, pargs):
        """
        launch daemon
        """
        self.control(pargs.pid,
                     pargs.target,
                     self.app.parser.error, 
                     pargs.action)


class Drain(Command):
    """
    Start a queue drain
    """
    res_py = staticmethod(utils.resolve)
    service = 'qubota.service.Drain'

    def get_config(self, candidate):
        # turn it into a dict
        return stuf(candidate)

    def get_parser(self, prog_name):
        parser = super(Drain, self).get_parser(prog_name)
        parser.formater_class = argparse.ArgumentDefaultsHelpFormatter
        parser.add_argument("-c", "--config", type=self.get_config, default=stuf(), help="Where to find the settings for the Drain")
        parser.add_argument('--queue', '-q', default=CLIApp.prefix, help="Queue name")
        parser.add_argument('--endpoint', '-e', default='tcp://127.0.0.1:5007', help="Work distribution endpoint")
        self.parser = parser
        return parser

    def take_action(self, pargs):
        config = pargs.config
        config.queue = self.app.queue(pargs.queue)
        config.domain = self.app.domain(pargs.queue)

        with utils.app(self.service, config) as app:
            app.serve_forever()


class Drone(Drain):
    """
    Start a worker
    """
    service = 'qubota.service.Drone'



def main(argv=sys.argv[1:], app=CLIApp):
    return app().run(argv)



