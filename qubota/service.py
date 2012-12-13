from . import sqs
from . import utils
from .job import Job
from .utils import reify
from .utils import resolve
from .zmqutils import Context
from .zmqutils import zmq
from circus.client import CircusClient
from contextlib import contextmanager
from ginkgo import Service
from ginkgo import Setting
from stuf import stuf
import ginkgo
import logging
import os
import signal
import sys
import time
import traceback
import uuid
import pprint


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
    circus_endpoint = 'tcp://127.0.0.1:5555'
    endpoint = Setting('endpoint', default=def_endpoint, 
                       help="0MQ dealer endpoint for distributing work")

    poll_interval = Setting('workforce_interval', 
                                  default=0.25,
                                  help="How often to wake up and check the workers")

    wait_interval = Setting('wait_interval', default=0.1)

    script = Setting('script', default='qubota')

    num_workers = Setting('num_workers', default=10)

    def __init__(self, config=None):
        super(Drain, self).__init__(config)
        self.reservations = {}

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
        out = self.cclient.call(self.ccmd(command, **props))
        if out['status'] != 'ok':
            self.log.error(out)
        return out

    @reify
    def watcher_def(self):
        return dict(name=self.prefix, 
                    cmd="bash -l -c 'qb drone --endpoint={} --queue={}'"\
                        .format(self.endpoint, self.queue.name),
                    shell=True,
                    numprocesses=self.num_workers)

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))

        signal.signal(signal.SIGINT, self.signal)
        signal.signal(signal.SIGTERM, self.signal)
        [self.async.spawn(self.incr).link(self.log_greenlet) for num in range(self.num_workers)]
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
        # if self.arbiter.alive:
        #     self.arbiter.stop_watchers(stop_alive=True)

        # self.arbiter.loop.stop()

        # # close sockets
        # self.arbiter.sockets.close_all()        
        pass

    def signal(self, *args):
        self.log.critical("SIG %s - exiting", args[0])
        self.stop()

    def listener(self):
        while self.running:
            self.log.debug('listen')
            msg = self.dealer.recv()
            self.log.debug(msg)
            self.process_msg(msg)
            self.async.sleep(self.wait_interval)

    def process_msg(self, msg):
        pass

    @property
    def running(self):
        return self.state.current in ['starting', 'ready']

    @reify
    def dealer(self):
        """
        lazy load dealer socket
        """
        self.log.info("Dealer initialized at: %s", self.endpoint)
        dealer = self.ctx.dealer(bind=self.endpoint)
        dealer.setsockopt(zmq.LINGER, 0)
        return dealer
  
    def do_reload(self):
        # - probably don't need right now
        pass

    def reserve_job(self, msg):
        with utils.log_tb(self.log, True):
            job = Job.from_map(msg.get_body())
            job.update_state('RESERVED')
            job.last_modified = time.time()
            self.domain.put_attributes(job.id, job)
            self.reserved[job.id] = job        
            self.queue.delete_message(msg)
            return job

    def dispatch(self, gr):
        job = gr.value
        # one out, one in
        self.async.spawn(self.incr).link(self.log_greenlet)
        self.dealer.send_json(dict(job))
        job.update_state('DISPATCHED')

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

            for msg, job in sqs.msgs(self.queue, num=self.num_workers):
                self.async.spawn(self.reserve_job, job).link(self.dispatch)

            self.async.sleep(self.poll_interval)            


class Drone(QService):
    """
    A process isolated worker
    """
    poll_interval = Setting('work_interval', default=0.1)
    drain = Setting('dealer', default=Drain.def_endpoint, help="Dealer endpont to get jobs")
    job_state = stuf(failure=False)
    resolve = staticmethod(resolve)

    def work(self):
        jm = self.drain.recv_json()
        job = Job.from_map(jm)
        self.drain.send_json(dict(ack=job.id))
        with self.girded_loins(job) as state:
            result = self.process_job(job)
            state.result = result

    def process_job(self, job):
        """
        resolve the job on the path, and run it

        #@@ add hooks 
        """
        job_callable = self.resolve(job.path)
        result = job_callable(*job.args.get('args', []), **job.args.get('kwargs', {}))
        return result 

    @contextmanager
    def girded_loins(self, job):
        state = self.job_state.copy()
        state.job = job.id
        try:
            yield state
        except Exception, e:
            state.failure = True
            state.tb = traceback.format_exc(e)
            self.log.error(state.tb)
        finally:
            st = dict(state)
            self.drain.send_json(st)
            self.log.info("state:\n%s", pprint.pformat(st))
            self.log.info("job:\n%s", pprint.pformat(dict(job)))
            if state.failure:
                sys.exit(1)
            sys.exit(0)

    def do_start(self):
        self.log.info("Starting %s: pid: %s" %(self.__class__.__name__, os.getpid()))
        self.drain = self.ctx.dealer(connect=self.drain)
        self.async.spawn(self.work)






