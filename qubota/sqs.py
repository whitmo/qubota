from boto.sqs.jsonmessage import JSONMessage


def msgs(queue, num=1, vistime=None, msgtype=JSONMessage):
    """
    Return messages in job queue
    """
    queue.set_message_class(msgtype)
    return queue.get_messages(num, vistime)
