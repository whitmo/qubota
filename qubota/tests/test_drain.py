from mock import patch
import unittest


class TestDrain(unittest.TestCase):

    def makeone(self, **config):
        from ..drain import Drain
        if config is None:
            config = {}
        return Drain(config)

    def test_initialize(self):
        drain = self.makeone()
        assert drain.max_workers == drain._defaults['max_workers'].default

    def test_initialize_w_config(self):
        drain = self.makeone(max_workers=25)
        assert drain.max_workers == 25

    def test_serve_forever_lifecycle(self):
        import gevent 
        drain = self.makeone(wait_interval=0.001)
        with patch.object(drain.config, 'queue'):
            gr = gevent.spawn(drain.serve_forever)
            gevent.sleep(0)
            assert gr.started
            assert not gr.dead
            assert len(drain.async._greenlets.greenlets) == 2
            for coro in drain.async._greenlets.greenlets:
                assert not coro.dead
            drain.stop()
            gevent.sleep(drain.wait_interval)
            assert gr.dead

