from .service import AbstractAsyncManager
import gevent
import gevent.coros
import gevent.event
import gevent.pool
import gevent.queue


class AsyncManager(AbstractAsyncManager):
    """
    # ganked from gingko
    """
    stop_timeout = 1

    def __init__(self):
        self._greenlets = gevent.pool.Group()

    def do_stop(self):
        if gevent.getcurrent() in self._greenlets:
            return gevent.spawn(self.do_stop).join()
        if self._greenlets:
            self._greenlets.join(timeout=self.stop_timeout)
            self._greenlets.kill(block=True, timeout=1)

    def spawn(self, func, *args, **kwargs):
        """Spawn a greenlet under this service"""
        return self._greenlets.spawn(func, *args, **kwargs)

    def spawn_later(self, seconds, func, *args, **kwargs):
        """Spawn a greenlet in the future under this service"""
        group = self._greenlets
        g = group.greenlet_class(func, *args, **kwargs)
        g.start_later(seconds)
        group.add(g)
        return g

    def sleep(self, seconds):
        return gevent.sleep(seconds)

    def queue(self, *args, **kwargs):
        return gevent.queue.Queue(*args, **kwargs)

    def timeout(self, time, exc=None):
        return gevent.Timeout(time, exc=None)

    # def event(self, *args, **kwargs):
    #     return gevent.event.Event(*args, **kwargs)

    # def lock(self, *args, **kwargs):
    #     return gevent.coros.Semaphore(*args, **kwargs)
