import gevent
import logging

log = logging.getLogger(__name__)


def simple_job(howlong=5, errout=False, interval=0.5):
    """
    This job wastes cpu cycles for the length of 'howlong'
    """
    print simple_job.uid
    if errout is True:
        errout = Exception("BAIL!!!")

    with gevent.Timeout(float(howlong), errout):
        log.warning("I'm awake")
        while True:
            gevent.sleep(interval)
            print("Makin' donuts")

    log.critical("And I'm out...")

simplejob = simple_job            





