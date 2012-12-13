import gevent
import logging

log = logging.getLogger(__name__)

def simple_job(howlong=240.0, errout=False, interval=10):
    """
    This job wastes cpu cycles for the length of 'howlong'
    """
    with gevent.Timeout(float(howlong), errout):
        log.warning("I'm awake")
        while True:
            gevent.sleep(interval)
            print "Makin' donuts"
    log.critical("And I'm out...")
            




