from boto.sqs.jsonmessage import JSONMessage
from stuf import stuf
import uuid


class Job(stuf):
    """
    A data object that describes a job
    """
    msg_ctor=JSONMessage

    def __init__(self, path=None, args=None, kwargs=None):
        self.id = str(uuid.uuid4())
        self.stdout = ''
        self.stdin = ''
        self.set(path, args, kwargs)
        self.state = 'NEW'

    def set(self, path, args, kwargs):
        self.args = dict(args=args, kwargs=kwargs)
        self.path = path

    @property
    def as_msg(self):
        """
        Converts job to a boto.sqs compatible message type
        """
        return self.msg_ctor(body=self)

    @classmethod
    def enqueue(job_ctor, mq, dbdom, sqs, path, args=None, kwargs=None):
        """
        Queue up a job in the awsq system

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
