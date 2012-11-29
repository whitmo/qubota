"""
Drains the queue, deals with the messages
"""
from gingko import Service
from gingko import Setting


class Workforce(Service):
    """
    Primary entry point for starting workforce of 1 - N workers
    """
    workforce_interval = Setting('workforce_interval', 
                                  default=0.1,
                                  help="How often to wake up and check the workers")

    num_workers = Setting('num_workers', default=20)

    def __init__(self, queue, domain):
        self.queue = queue
        self.domain = domain
        self.add_service()

    def do_start(self):
        # launch arbiter
        self.spawn(self.worker_loop)

    def do_stop(self):
        # - check status of jobs
        # - exits
        pass

    def do_reload(self):
        # - probably don't need right now
        pass

    def worker_loop(self):
        while True:
            # get jobs
            # validate? (test imports?)
            # farm out via dealer socket
            # check worker pool / restart if necessary
            self.async.sleep(self.workforce_interval)


class Worker(Service):
    """
    A process isolated worker
    """
    work_interval = Setting('work_interval', default=20)

    def __init__(self, queue, domain):
        self.queue = queue
        self.domain = domain

    def work(self):
        while True:
            self.async.sleep(self.work_interval)

    def do_start(self):
        self.async.spawn(self.work)
