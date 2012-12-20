#from multiprocessing import Process
#from multiprocessing import process
#import multiprocessing as mp
from . import sqs
from . import utils
from .job import Job
from .utils import reify
from .utils import resolve
from .zmqutils import Context
from .zmqutils import zmq
from Queue import Empty
from contextlib import contextmanager
from gevent import Greenlet
from gevent.queue import Queue
from ginkgo import Service
from ginkgo import Setting
from stuf import stuf
from zmq.core.error import ZMQError
import ginkgo
import logging
import os
import pprint as pp
import sys
import time
import traceback
import base64


class Drain(Service):
    """
    Drains the queue, distibutes the work.

    Primary entry point for starting workforce of 1 - N workers
    """
    def_endpoint = 'tcp://127.0.0.1:5008'
    def_listen_endpoint = 'tcp://127.0.0.1:5009'
    circus_endpoint = 'tcp://127.0.0.1:5555'
    endpoint = Setting('endpoint', default=def_endpoint, 
                       help="0MQ dealer endpoint for distributing work")
    listen_endpoint = Setting('listen_endpoint', default=def_listen_endpoint, 
                       help="0MQ dealer endpoint for distributing work")

    poll_interval = Setting('poll_interval', 
                            default=0.25,
                            help="How often to wake up and check the sqs queue")

    wait_interval = Setting('wait_interval', default=0.01, 
                            help="How long to pause after processing a worker response")

    queue = Setting('queue', default='spec:qubota.cli.CLIApp.prefix', 
                    help="A string, a specifier to something "\
                        "on the python path or an actual SQS queue instance")

    sqs_size = Setting('sqs_size', default=10, 
                       help="how many messages to yank off sqs queue at a time")

    domain = Setting('domain', default='spec:qubota.cli.CLIApp.prefix', 
                     help="A string, a specifier to something on the "\
                         "python path or an actual SDB domain")

    log = logging.getLogger(__name__)

    resolve = staticmethod(resolve)

    def __init__(self, config=None):
        self.config = ginkgo.settings
        if isinstance(config, dict):
            self.config.load(config)
        self.root_pid = os.getpid()
        super(Drain, self).__init__(config)

        self._ctor_cache = {}
        self.jobs = {}
        self.result_queue = Queue()
        #mp.log_to_stderr(logging.DEBUG)

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        self.async.spawn(self.sqs_loop)
        self.async.spawn(self.listener) # optional 0mq loop
        self.async.spawn(self.result_loop)

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
            # if self.root_pid != os.getpid():
            #     raise ValueError('%s %s' %(self.root_pid, os.getpid()))
            with self.catch_exc('result_loop') as log:
                try:
                    log.job = job = self.result_queue.get(block=False)
                    self._process_msg(job)
                except Empty:
                    pass
            self.async.sleep(self.wait_interval)

    def listener(self):
        while self.running:
            with self.catch_exc('listener') as logout:
                logout.msg = msg = self.puller.recv_json()
                self.process_msg(msg)

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
        # replace with dispatch tree
        if msg.state == 'NEW':
            msg.internal = True
            return self._reserve_job(msg)
            
        if msg.state in self.DONE:
            orig, gr = self.jobs.pop(msg.id)
            orig.update_state(msg.state)
            if 'msg' in orig:
                self.log.info("Deleting: %s" %msg)
                self.queue.delete_message(msg.msg)

    @property
    def running(self):
        return self.state.current in ['starting', 'ready']

    def initialize_socket(self, sock_type, endpoint):
        sock = None
        try:
            sock = sock_type(bind=endpoint)
            sock.setsockopt(zmq.LINGER, 0)
            self.async.sleep(0.02)
        except ZMQError, e:
            self.log.exception("%s:%s, Exiting", e, endpoint)
            sys.exit(1)
        return sock

    @reify
    def puller(self):
        """
        lazy load dealer socket
        """
        ctx = Context()
        self.log.info("Puller initialized: %s", self.endpoint)
        sock = self.initialize_socket(ctx.pull, self.endpoint)
        return sock

    def _reserve_job(self, job, msg=None):
        job.domain = self.domain
        if msg:
            job.msg = msg
        self.jobs[job.id] = job
        job.update_state('CLAIMED')
        gr = Drone.spawn(job, self.result_queue)
        self.jobs[job.id] = job, gr,

    def reserve_job(self, msg):
        with utils.log_tb(self.log, True):
            mbody = msg.get_body()
            job = Job.from_map(mbody)
            self._reserve_job(job, msg)
            return job

    def sqs_loop(self):
        """
        suck down sqs messages and distribute them
        """
        while True:
            with self.catch_exc('sqs loop'):
                for job in sqs.msgs(self.queue, num=self.sqs_size):
                    self.async.spawn(self.reserve_job, job)
            self.async.sleep(self.poll_interval)            


class Drone(Greenlet):
    """
    A process isolated worker
    """
    drain = Drain.def_endpoint
    reply_endpoint = Drain.def_listen_endpoint
    job_status = stuf(success=False)
    resolve = staticmethod(resolve)

    def __init__(self, job, queue):
        self.queue = queue
        self.job = job
        Greenlet.__init__(self)

    @classmethod
    def spawn(cls, job, queue):
        drone = cls(job, queue)
        drone.start()
        return drone

    @reify
    def log(self):
        return logging.getLogger(__name__)

    def _run(self):
        #self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        with self.girded_loins(self.job):
            return self.process_job(self.job)

    def process_job(self, job):
        """
        resolve the job on the path, and run it

        #@@ add hooks 
        """
        job_callable = self.resolve(job.path)
        job_callable.uid = job.id
        kwargs = job.args.get('kwargs', {}) or {}
        args = job.args.get('args', []) or []
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
            job.update_state('FAILED')
            tb = traceback.format_exc(e).encode('zlib')
            job.tb = base64.encodestring(tb)
            self.log.exception("Job failure by exception:\n%s", pp.pformat(job))
        finally:
            job.update_state('COMPLETED')
            job.duration = time.time() - start
            self.queue.put(job)
            self.log.debug("job:\n%s", pp.pformat(dict(job)))




