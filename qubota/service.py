from . import sqs
from .job import Job
from .utils import resolve
from .utils import reify
from .zmqutils import Context
from .zmqutils import zmq
from circus import get_arbiter
from ginkgo import Service
from ginkgo import Setting
from stuf import stuf
import ginkgo
import logging
import sys
import time
import traceback
import uuid
import os
import signal


class QService(Service):
    queue = Setting('queue', default='spec:qubota.cli.CLIApp.prefix', 
                    help="A string, a specifier to something "\
                        "on the python path or an actual SQS queue instance")

    domain = Setting('domain', default='spec:qubota.cli.CLIApp.prefix', 
                    help="A string, a specifier to something on the "\
                         "python path or an actual SDB domain")

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
    def_endpoint = 'tcp://127.0.0.1:5007'
    prefix = ':drone:'
    proc_name="qubota.drain"
    endpoint = Setting('endpoint', default=def_endpoint, 
                       help="0MQ dealer endpoint for distributing work")

    poll_interval = Setting('workforce_interval', 
                                  default=0.25,
                                  help="How often to wake up and check the workers")

    wait_interval = Setting('wait_interval', default=0.1)

    script = Setting('script', default='qubota')

    num_workers = Setting('num_workers', default=10)

    get_arbiter = staticmethod(get_arbiter)

    def __init__(self, config=None):
        super(Drain, self).__init__(config)
        self.reservations = {}

    @reify
    def drone_watch(self):
        #staling?
        self.arbiter.get_watcher(self.prefix)

    @reify
    def watcher_def(self):
        return dict(name=self.prefix, cmd='qubota drone',
                    args="--endpoint={} --queue={}"\
                        .format(self.endpoint, self.queue.name),
                    numprocesses=self.num_workers)

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        self.arbiter = get_arbiter([self.watcher_def], 
                                   context=self.ctx,
                                   proc_name=self.proc_name,
                                   stream_backend='gevent')
        signal.signal(signal.SIGINT, self.signal)
        signal.signal(signal.SIGTERM, self.signal)
        self.spawn(self.worker_loop)
        self.spawn(self.listener)

    def do_stop(self):
        if self.arbiter.alive:
            self.arbiter.stop_watchers(stop_alive=True)

        self.arbiter.loop.stop()

        # close sockets
        self.arbiter.sockets.close_all()        


    def signal(self, *args):
        self.log.critical("SIG %s - exiting", args[0])
        self.stop()

    def listener(self):
        while self.running:
            msg = self.dealer.recv()
            self.process_msg(msg)
            self.async.sleep(self.wait_interval)

    def process_msg(self, msg):
        pass

    @property
    def running(self):
        return self.state.current in ['starting', 'ready']

    @property
    def worker_watchers(self):
        return (watch for watch in self.arbiter.iter_watchers \
                    if watch.name.startswith(self.prefix))

    @reify
    def dealer(self):
        """
        lazy load dealer socket
        """
        dealer = self.ctx.dealer(bind=self.endpoint)
        dealer.setsockopt(zmq.LINGER, 0)
        return dealer
  
    def do_reload(self):
        # - probably don't need right now
        pass

    def reserve_job(self, msg):
        #@@ error catching
        job = Job.from_map(msg.get_body())
        job.update_state('RESERVED')
        job.last_modified = time.time()
        self.domain.put_attributes(job.id, job)
        self.reserved[job.id] = job        
        self.queue.delete_message(msg)
        return job

    def dispatch(self, job):
        # one out, one in
        self.drone_watcher.incr()
        self.dealer.send_json(dict(job))
        job.update_state('DISPATCHED')

    def worker_loop(self):
        """
        - check for minimum # of workers

        - wait for ack / note who grabbed it
        - monitor worker pool / restart if necessary
        """
        while True:
            for msg, job in sqs.msgs(self.queue, num=self.num_workers):
                self.async.spawn(self.reserve_job, job).link(self.dispatch)
            self.async.sleep(self.poll_interval)            


class Drone(QService):
    """
    A process isolated worker
    """
    poll_interval = Setting('work_interval', default=0.1)
    drain = Setting('dealer', default=Drain.def_endpoint, help="Dealer endpont to get jobs")
    job_state = stuf(success=False)
    resolve = staticmethod(resolve)

    def work(self):
        job = Job.from_map(self.drain.recv_json())
        self.drain.send_json(dict(ack=job.id))
        with self.girded_loins() as state:
            result = self.process_job(job)
            state.update(result)

    def process_job(self, job):
        """
        resolve the job on the path, and run it

        #@@ add hooks 
        """
        job_callable = self.resolve(job.path)
        result = job_callable(*job.args, **job.kwargs)
        return result

    def girded_loins(self, job):
        state = self.job_state.copy()
        state.job = job.id
        try:
            yield state
        except Exception, e:
            state.failure = True
            state.tb = traceback.format_exc(e)
        finally:
            self.drain.send_json(dict(state))
            if state.success:
                sys.exit(0)
            sys.exit(1)

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        self.drain = self.ctx.dealer(connect=self.drain)
        self.async.spawn(self.work)


