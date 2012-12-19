from . import sqs
from . import utils
from .job import Job
from .utils import reify
from .utils import resolve
from .zmqutils import Context
from .zmqutils import zmq
from Queue import Empty
from contextlib import contextmanager
from gevent.queue import Queue
from ginkgo import Service
from ginkgo import Setting
from multiprocessing import Process
from multiprocessing import process
from stuf import stuf
from zmq.core.error import ZMQError
import ginkgo
import logging
import multiprocessing as mp
import os
import pprint as pp
import sys
import time
import traceback



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

        super(Drain, self).__init__(config)

        self._ctor_cache = {}
        self.reserved = {}
        self.outgoing = Queue()
        self.result_queue = mp.Queue()
        #mp.log_to_stderr(logging.DEBUG)

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        self.async.spawn(self.dispatcher)
        self.async.spawn(self.sqs_loop)
        self.async.spawn(self.listener)
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

    DONE = {'COMPLETED', 'FAILED'}

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
            return self.async.spawn(self._reserve_job, msg).link(self.queue_for_dispatch)
            
        if msg.state in self.DONE:
            orig = self.reserved.pop(msg.id)

            try:
                if orig.proc.is_alive():
                    orig.proc.terminate()
            except OSError:
                pass
            except AssertionError, e:
                self.log.exception("PID wierdness: %s", e)

            orig.report = msg.status
            orig.update_state(msg.state)
            if 'msg' in orig:
                self.log.info("Deleting: %s" %msg)
                self.queue.delete_message(msg)

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
        self.reserved[job.id] = job
        job.update_state('RESERVED')
        return job

    def reserve_job(self, msg):
        with utils.log_tb(self.log, True):
            mbody = msg.get_body()
            job = Job.from_map(mbody)
            job = self._reserve_job(job, msg)
            return job

    def dispatch_job(self, job):
        job.update_state('DISPATCHED')
        jc = job.copy()
        del jc.domain
        proc = Drone.spawn(jc, self.result_queue) # make this pluggable?!
        job.proc = proc
        job.proc.start()

    def queue_for_dispatch(self, gr):
        job = gr.value
        self.outgoing.put(('dispatch_job', job))

    def dispatcher(self):
        """
        Check the internal queue `outgoing` for things to dispatch
        """
        while True:
            with self.catch_exc('dispatcher'):
                method, job = self.outgoing.get()
                getattr(self, method)(job)

    def sqs_loop(self):
        """
        suck down sqs messages and distribute them
        """
        while True:
            with self.catch_exc('sqs loop'):
                for job in sqs.msgs(self.queue, num=self.sqs_size):
                    self.async.spawn(self.reserve_job, job).link(self.queue_for_dispatch)
            self.async.sleep(self.poll_interval)            


class Drone(Process):
    """
    A process isolated worker
    """
    drain = Drain.def_endpoint
    reply_endpoint = Drain.def_listen_endpoint
    job_status = stuf(success=False)
    resolve = staticmethod(resolve)

    def initialize(self, job, queue, **kw):
        self.job = job
        self.queue = queue
        self._kwargs = kw # unused

    def is_alive(self):
        '''
        Return whether process is alive
        '''
        if self is process._current_process:
            return True

        pid = os.getpid()
        assert self._parent_pid == pid, 'can only test a child process: %s %s' %(self._parent_pid, pid)
        if self._popen is None:
            return False
        self._popen.poll()
        return self._popen.returncode is None
    
    @classmethod
    def spawn(cls, job, result_queue, **kw):
        proc = cls()
        proc.initialize(job, result_queue)
        return proc

    @reify
    def log(self):
        return logging.getLogger(__name__)

    def run(self):
        #@@ eventually process args, kwargs here
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        with self.girded_loins(self.job) as status:
            result = self.process_job(self.job)
            status.result = result

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
        status = job.status = self.job_status.copy()
        status.start = start = time.time()
        job.update_state('STARTED')        

        try:
            yield status
            status.success = True
        except Exception, e:
            job.update_state('FAILED')
            status.success = False
            status.tb = traceback.format_exc(e)
            self.log.exception("Job failure by exception:\n%s", pp.pformat(job))
        finally:
            if 'domain' in job:
                del job.domain

            if 'proc' in job:
                del job.proc

            job.update_state('COMPLETED')

            status.duration = time.time() - start
            
            self.queue.put(job)

            st = dict(status)
            self.log.debug("status:\n%s", pp.pformat(st))
            self.log.debug("job:\n%s", pp.pformat(dict(job)))

            if status.success:
                sys.exit(1)
            sys.exit(0)

