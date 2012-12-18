from . import sqs
from . import utils
from .job import Job
from .utils import reify
from .utils import resolve
from .zmqutils import Context
from .zmqutils import zmq
from circus.client import CircusClient
from circus.exc import CallError
from contextlib import contextmanager
from gevent.queue import Queue
from ginkgo import Service
from ginkgo import Setting
from multiprocessing import Process
from stuf import stuf
from zmq.core.error import ZMQError
import gevent
import ginkgo
import logging
import os
import pprint as pp
import signal
import sys
import time
import traceback
import uuid


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


    poll_interval = Setting('workforce_interval', 
                            default=0.25,
                            help="How often to wake up and check the workers")

    wait_interval = Setting('wait_interval', default=0.1)

    script = Setting('script', default='qubota')

    num_workers = Setting('num_workers', default=10)
    with_circus = Setting('with_circus', default=False)

    queue = Setting('queue', default='spec:qubota.cli.CLIApp.prefix', 
                    help="A string, a specifier to something "\
                        "on the python path or an actual SQS queue instance")

    domain = Setting('domain', default='spec:qubota.cli.CLIApp.prefix', 
                    help="A string, a specifier to something on the "\
                         "python path or an actual SDB domain")

    endpoint = Setting('daemon', default=False, help="Daemonize")

    log = logging.getLogger(__name__)

    pull_interval = 0.025

    def __init__(self, config=None):
        self.config = ginkgo.settings

        if isinstance(config, dict):
            self.config.load(config)
        self.socks = stuf()
        self._id = uuid.uuid4().hex
        self.ctx = Context.instance()
        super(Drain, self).__init__(config)
        self.reserved = {}
        self.outgoing = Queue()

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))

        self.async.spawn(self.dispatcher)
        self.async.spawn(self.worker_loop)
        self.async.spawn(self.listener)

    def log_greenlet(self, gr):
        self.log.debug(gr.value)

    def listener(self):
        while self.running:
            self.log.debug('listen')
            msg = self.puller.recv_json()
            self.log.debug(msg)
            self.process_msg(msg)
            self.async.sleep(self.pull_interval)

    def process_msg(self, msg):
        # delete job
        # record result
        pass

    @property
    def running(self):
        return self.state.current in ['starting', 'ready']

    def initialize_socket(self, sock_type, endpoint, name):
        self.log.info("%s initialized at: %s", name, endpoint)
        try:
            sock = sock_type(bind=endpoint)
        except ZMQError, e:
            self.log.exception("%s:%s, Exiting", e, endpoint)
            sys.exit(1)
        sock.setsockopt(zmq.LINGER, 0)
        self.async.sleep(0.2)
        return sock

    @reify
    def puller(self):
        """
        lazy load dealer socket
        """
        sock = self.initialize_socket(self.ctx.pull, self.listen_endpoint, 'Dealer')
        return sock
  
    def do_reload(self):
        # - probably don't need right now
        pass

    def reserve_job(self, msg):
        with utils.log_tb(self.log, True):
            mbody = msg.get_body()
            job = Job.from_map(mbody)
            job.domain = self.domain
            job.update_state('RESERVED')
            self.reserved[job.id] = (job, msg)        
            #self.queue.delete_message(msg)
            return job

    def dispatch_job(self, gr):
        job = gr.value
        job.update_state('DISPATCHED')

    def queue_for_dispatch(self, gr):
        self.outgoing.put(('dispatch_job', gr))

    def dispatcher(self):
        while True:
            method, job_gr = self.outgoing.get()
            getattr(self, method)(job_gr)

    def worker_loop(self):
        """
        - check for minimum # of workers

        - wait for ack / note who grabbed it
        - monitor worker pool / restart if necessary
        """
        start = time.time()
        while True:
            if int(start) % 100 == 0:
                self.log.debug('wake up: check msgs')

            for job in sqs.msgs(self.queue, num=self.num_workers):
                self.async.spawn(self.reserve_job, job).link(self.queue_for_dispatch)
            self.async.sleep(self.poll_interval)            


class Drone(Process):
    """
    A process isolated worker
    """
    drain = Drain.def_endpoint
    reply_endpoint = Drain.def_listen_endpoint
    job_state = stuf(success=False)
    resolve = staticmethod(resolve)

    def initialize(self, job, kwargs):
        self.job = job
    
    @classmethod
    def spawn(cls, *args, **kw):
        proc = cls(args=args, kwargs=kw)
        proc.initialize(args[0], kw)
        return proc

    @reify
    def log(self):
        return logging.getLogger(__name__)

    def run(self):
        #@@ eventually process args, kwargs here
        self.ctx = Context.instance()
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        self.drain = self.ctx.push(connect=self.drain)
        self.drain.setsockopt(zmq.LINGER, 0)

        # self.reply = self.ctx.dealer(connect=self.reply_endpoint)
        # self.reply.setsockopt(zmq.LINGER, 0)
        worker = gevent.spawn(self.work)
        worker.join()

    def work(self):
        with self.girded_loins(self.job) as state:
            result = self.process_job(self.job)
            state.result = result

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
        state = self.job_state.copy()
        job.state = state
        try:
            yield state
        except Exception, e:
            state.success = False
            state.tb = traceback.format_exc(e)
            self.log.error(state.tb)
        finally:
            self.drain.send_json(job)
            st = dict(state)
            self.log.info("state:\n%s", pp.pformat(st))
            self.log.info("job:\n%s", pp.pformat(dict(job)))

            if state.success:
                sys.exit(1)
            sys.exit(0)
