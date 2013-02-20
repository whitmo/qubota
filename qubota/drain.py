from .utils import reify
from . import sqs
from . import utils
from .job import Job
from .service import Service
from .service import Setting
from Queue import Empty
from contextlib import contextmanager
from stuf import stuf
import base64
import pprint as pp
import sys
import time
import traceback
import os


@Setting.initialize_all
class Drain(Service):
    """
    Drains the queue, distibutes the work.

    Primary entry point for starting workforce of 1 - N workers
    """
    max_workers = Setting(default=50, # tune this
                          help="How many workers to allow at a time")

    start_timeout = Setting(default=0.05,
                            help="how long to wait for spin up before failing")

    poll_interval = Setting(default=0.25,
                            help="How often to wake up and check the sqs queue")

    wait_interval = Setting(default=0.01, 
                            help="How long to pause after processing a worker response")

    queue = Setting(default='spec:qubota.cli.CLIApp.prefix', 
                    help="A string, a specifier to something "\
                        "on the python path or an actual SQS queue instance")

    sqs_size = Setting(default=3, 
                       help="how many messages to yank off sqs queue at a time")

    domain = Setting(default='spec:qubota.cli.CLIApp.prefix', 
                     help="A string, a specifier to something on the "\
                         "python path or an actual SDB domain")

    async_handler = Setting(default='qubota.green.AsyncManager', 
                            help="dotted name of async manager")

    debug = Setting(default=False, 
                    help="a boolean, when set to true, enables postmortem debugging")

    def __init__(self, config=None):
        super(Drain, self).__init__(config)
        self._ctor_cache = {}
        self.jobs = {}
        self.result_queue = self.async.queue()
        #mp.log_to_stderr(logging.DEBUG)

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        self.async.spawn(self.reception_loop)
        self.async.spawn(self.result_loop)
        
    def do_stop(self):
        self.log.info('Stopping')
        self.async.do_stop()
        self.state = "stopped"

    @contextmanager
    def catch_exc(self, name=None):
        status = stuf()
        try:
            yield status
        except Exception, e:
            import pdb; pdb.post_mortem(sys.exc_info()[2])
            self.log.exception('%s failed: %s\n %s\n', 
                               name, e, pp.pformat(dict(status)))
        except KeyboardInterrupt:
            self.stop()
            sys.exit(0)

    def result_loop(self):
        while self.running:
            with self.catch_exc('result_loop') as log:
                try:
                    log.job = job = self.result_queue.get(block=False)
                    self._process_msg(job)
                except Empty:
                    pass
            self.async.sleep(self.wait_interval)

    def spec_to_ctor(self, spec):
        ctor = self._ctor_cache.get(spec, None)
        if ctor is None:
            ctor = self._ctor_cache[spec] = self.resolve(spec)
        if ctor is None:
            ctor = stuf
        return ctor

    DONE = {'COMPLETED', 'FAILED', 'NOTFOUND'}

    def process_msg(self, msg):
        # all msg are jobs
        if 'ctor' in msg:
            ctor = self.spec_to_ctor(msg['ctor'])
            msg = ctor(msg)
        self._process_msg(msg)

    def _process_msg(self, msg):
        #@@ replace with dispatch tree?
        if msg.state == 'NEW':
            msg.internal = True
            return self._reserve_job(msg)
            
        if msg.state in self.DONE:
            orig, gr = self.jobs.pop(msg.id)
            orig.update_state(msg.state)
            if 'msg' in orig:
                self.log.info("Deleting: %s" %msg)
                self.queue.delete_message(msg.msg)

    def _reserve_job(self, job, msg=None):
        job.domain = self.domain
        if msg:
            job.msg = msg
        self.jobs[job.id] = job
        job.update_state('CLAIMED')
        gr = self.async.spawn(JobRun(job, self.result_queue, self))
        self.jobs[job.id] = job, gr,

    def pull_messages(self):
        """
        template method for pulling message from a source, in this
        case from sqs
        """
        return sqs.msgs(self.queue, num=self.sqs_size)

    def reserve_job(self, msg):
        with utils.log_tb(self.log, True):
            job = self.msg_to_job(msg)
            self._reserve_job(job, msg)
            return job

    def msg_to_job(self, msg):
        """
        Derserialize a possible sqs payload into a job
        """
        if not isinstance(msg, dict):
            msg = msg.get_body()
        job = Job.from_map(msg)
        return job

    def reception_loop(self):
        """
        suck down sqs messages and distribute them
        """
        while True:
            with self.catch_exc('sqs loop'):
                for job in self.pull_messages():
                    self.reserve_job(job)
            self.async.sleep(self.poll_interval)            


class JobRun(object):
    """
    Represents a single execution of a job
    """
    job_status = stuf(success=False)

    def __init__(self, job, queue, parent):
        self.queue = queue
        self.job = job
        self.parent = parent

    @reify
    def log(self):
        return self.parent.log

    def __call__(self):
        with self.girded_loins(self.job):
            return self.process_job(self.job)

    def process_job(self, job):
        """
        resolve the job on the path, and run it

        #@@ add hooks 
        """
        job_callable = self.parent.resolve(job.path)
        
        try:
            job_callable.uid = job.id
        except AttributeError:
            self.log.warn('Unable to annotate callable with job uid:%s' %job.id)

        kwargs = job.args.get('kwargs', {}) or {}
        args = job.args.get('args', []) or []

        if isinstance(args, basestring):
            raise TypeError('Job arguments must be a non-string iterable')

        result = job_callable(*args, **kwargs)
        return result 

    @contextmanager
    def girded_loins(self, job):
        """
        All the error catching and status reporting
        """
        job.update_state('STARTED')        
        job.start = start = time.time()
        try:
            yield 
            job.success = True
        except ImportError:
            job.update_state('NOTFOUND')
        except Exception, e:
            tb = traceback.format_exc(e).encode('zlib')
            job.tb = base64.encodestring(tb)
            job.exc = repr(e)
            job.update_state('FAILED')
            self.log.exception("Job failure by exception:\n%s", pp.pformat(job))
        finally:
            job.update_state('COMPLETED')
            job.duration = time.time() - start
            self.queue.put(job)
            self.log.debug("job:\n%s", pp.pformat(dict(job)))
