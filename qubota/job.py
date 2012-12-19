from boto.sqs.jsonmessage import JSONMessage
from gevent.coros import RLock
from stuf import stuf
import json
import time
import uuid


class Job(stuf):
    """
    A data object that describes a job
    """
    msg_ctor=JSONMessage
    domain_lock = RLock()

    def __init__(self, path=None, args=[], kwargs={}):
        self.ctor = ".".join((self.__class__.__module__, 
                              self.__class__.__name__, 
                              'from_map'))
        self.id = str(uuid.uuid4())
        self.set(path, args, kwargs)
        self.update_state('NEW')
        self.run_info = None
        self.start = None
        self.duration = None
        self.success = False
        self.tb = None

    def set(self, path, args, kwargs):
        self.args = dict(args=args, kwargs=kwargs)
        self.path = path

    def update_state(self, new):
        self.state = new
        self.last_modified = time.time()
        if 'domain' in self:
            with self.domain_lock:
                out = dict(self)
                #out.pop('domain')
                self.domain.put_attributes(self.id, out)
        return self

    @property
    def as_msg(self):
        """
        Converts job to a boto.sqs compatible message type
        """
        return self.msg_ctor(body=dict(self))

    @classmethod
    def from_map(cls, mapping):
        inst = cls()
        [inst.__setitem__(key, mapping[key]) for key in mapping] #stuf.update is fuxored
        return inst

    def copy(self):
        """
        fix issue with stuf copy
        """
        return self.from_map(self)

    from_dict = from_map

    @classmethod
    def enqueue(job_ctor, mq, dbdom, path, args=None, kwargs=None):
        """
        Queue up a job in the qubota system

        * mq: a `boto.sqs.queue` instance
        * dbdom: a `boto.sdb.domain` instance
        * path: a dotted name specification for a python callable
        * args: a list of arguments to pass into callable
        * kwargs: a mapping of key word argumemts
        """
        job = job_ctor(path, args, kwargs)
        mq.write(job.as_msg)
        dbdom.put_attributes(job.id, job)
        return job


