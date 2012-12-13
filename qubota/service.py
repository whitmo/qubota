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
from stuf import stuf
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


class QService(Service):
    queue = Setting('queue', default='spec:qubota.cli.CLIApp.prefix', 
                    help="A string, a specifier to something "\
                        "on the python path or an actual SQS queue instance")

    domain = Setting('domain', default='spec:qubota.cli.CLIApp.prefix', 
                    help="A string, a specifier to something on the "\
                         "python path or an actual SDB domain")

    endpoint = Setting('daemon', default=False, help="Daemonize")

    log = logging.getLogger(__name__)

    def __init__(self, config=None):
        self.config = ginkgo.settings

        if isinstance(config, dict):
            self.config.load(config)
        self.socks = stuf()
        self._id = uuid.uuid4().hex
        self.ctx = Context.instance()
        


class Drain(QService):
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
    drones_up = Setting('drones_up', default=False)

    def __init__(self, config=None):
        super(Drain, self).__init__(config)
        self.reserved = {}
        self.outgoing = Queue()

    @classmethod
    def ctor(cls, yaml):
        return cls(yaml)

    @reify
    def cclient(self):
        #@@ add ssh support??
        self.log.info("Circus client: %s" %self.circus_endpoint)
        return CircusClient(endpoint=self.circus_endpoint)

    @staticmethod
    def ccmd(command, **props):
        return dict(command=command, properties=props)

    def circus_call(self, command, **props):
        try:
            out = self.cclient.call(self.ccmd(command, **props))
        except CallError:
            self.log.exception("Failed call %s:%s to %s", command, 
                               pp.pformat(props), self.circus_endpoint)

            self.async.sleep(2)

            self.log.warning("Retry msg %s:%s to %s", command, 
                             pp.pformat(props), self.circus_endpoint)
            out = self.circus_call(command, **props)

        if out['status'] != 'ok':
            self.log.error(pp.pformat(out))
        return out

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        signal.signal(signal.SIGINT, self.signal)
        signal.signal(signal.SIGTERM, self.signal)

        if self.drones_up:
            self.log.info("Bring %d drones up", self.num_workers)
            gr = self.async.spawn(self.incr, howmany=self.num_workers)
            gr.link(self.log_greenlet); gr.join()

        self.async.spawn(self.dispatcher)
        self.async.spawn(self.worker_loop)
        self.async.spawn(self.listener)

    def incr(self, howmany=1):
        """
        Bump up the drone count
        """
        return self.circus_call('incr', name='drone', nbprocess=howmany)

    def log_greenlet(self, gr):
        self.log.debug(gr.value)

    def do_stop(self):
        pass

    def signal(self, *args):
        self.log.critical("Got SIG %s - ciao!", args[0])
        self.stop()

    def listener(self):
        while self.running:
            self.log.debug('listen')
            msg = None
            msg = self.dealer.recv_json()

            if msg is not None:
                self.log.debug(msg)
                self.process_msg(msg)

            self.async.sleep(self.wait_interval)

    def process_msg(self, msg):
        pass

    @property
    def running(self):
        return self.state.current in ['starting', 'ready']

    @reify
    def pusher(self):
        """
        lazy load dealer socket
        """
        self.log.info("Pusher initialized at: %s", self.listen_endpoint)
        dealer = self.ctx.push(bind=self.endpoint)
        dealer.setsockopt(zmq.LINGER, 0)
        self.async.sleep(0.2)
        return dealer

    @reify
    def dealer(self):
        """
        lazy load dealer socket
        """
        self.log.info("Dealer initialized at: %s", self.endpoint)
        dealer = self.ctx.dealer(bind=self.listen_endpoint)
        dealer.setsockopt(zmq.LINGER, 0)
        self.async.sleep(0.2)
        return dealer
  
    def do_reload(self):
        # - probably don't need right now
        pass

    def reserve_job(self, msg):
        with utils.log_tb(self.log, True):
            mbody = msg.get_body()
            job = Job.from_map(mbody)
            job.update_state('RESERVED')
            job.last_modified = time.time()
            self.domain.put_attributes(job.id, dict(job))
            self.reserved[job.id] = job        
            self.queue.delete_message(msg)
            return job

    def dispatch_job(self, gr):
        job = gr.value
        # one out, one in @@ need a limit here

        if self.drones_up:
            self.async.spawn(self.incr).link(self.log_greenlet)

        self.log.info(pp.pformat(dict(job)))
        self.pusher.send_json(dict(job), flags=zmq.NOBLOCK)
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


class Drone(QService):
    """
    A process isolated worker
    """
    poll_interval = Setting('work_interval', default=0.1)
    drain = Setting('dealer', default=Drain.def_endpoint, help="Dealer endpont to get jobs")
    reply_endpoint = Setting('reply_endpoint', default=Drain.def_listen_endpoint, 
                       help="0MQ dealer endpoint for distributing work")
    job_state = stuf(success=False)
    resolve = staticmethod(resolve)

    def work(self):
        jm = self.drain.recv_json()
        job = Job.from_map(jm)
        self.reply.send_json(dict(ack=job.id))
        with self.girded_loins(job) as state:
            result = self.process_job(job)
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
        state.job = job.id
        try:
            yield state
        except Exception, e:
            state.success = False
            state.tb = traceback.format_exc(e)
            self.log.error(state.tb)
        finally:
            st = dict(state)
            self.reply.send_json(st)
            self.log.info("state:\n%s", pp.pformat(st))
            self.log.info("job:\n%s", pp.pformat(dict(job)))
            if state.success:
                sys.exit(1)
            sys.exit(0)

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        self.drain = self.ctx.pull(connect=self.drain)
        self.drain.setsockopt(zmq.LINGER, 0)
        self.reply = self.ctx.dealer(connect=self.reply_endpoint)
        self.reply.setsockopt(zmq.LINGER, 0)
        self.async.spawn(self.work)






