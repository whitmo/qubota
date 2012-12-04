import gevent


def simple_job(howlong=240, errout=False, interval=10):
    """
    This job wastes cpu cycles for the length of 'howlong'
    """
    with gevent.Timeout(howlong, errout)):
        while True:
            gevent.sleep(interval)
        



