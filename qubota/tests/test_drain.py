from ..job import Job
from contextlib import contextmanager
from mock import patch
import gevent 
import unittest


class TestJob(Job):
    msg_ctor = dict


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

    @contextmanager
    def drain_run(self, drain):
        with drain:
            gr = drain.async.spawn(drain.serve_forever)
            drain.async.sleep(0)
            yield gr

    @classmethod
    def set_global(cls, globname, **kw):
        setattr(cls, "result_%s" %globname, kw)

    wait = 0.00000000000000001 # still need a wee bit of time to switch through coroutines

    def test_a_job_run(self):
        drain = self.makeone(start_timeout=self.wait, 
                             poll_interval=0.01, 
                             wait_interval=0.01)
        with patch.object(drain.config, 'queue'), patch.object(drain.config, 'domain'):         
            with self.drain_run(drain):
                name = 'test_a_job_run'
                drain._reserve_job(TestJob(path='qubota.tests.test_drain.TestDrain.set_global', 
                                           args=[name], 
                                           kwargs=dict(hi=True)))
                gevent.sleep(self.wait)
                out = getattr(self, "result_%s" %name, None)
                assert out
    
    def test_a_job_run_arg_exc(self):
        drain = self.makeone(start_timeout=self.wait, 
                             wait_interval=self.wait, 
                             poll_interval=self.wait)
        with patch.object(drain.config, 'queue'), patch.object(drain.config, 'domain') as dom:         
            with self.drain_run(drain):
                name = 'test_a_job_run'
                drain._reserve_job(TestJob(path='qubota.tests.test_drain.TestDrain.set_global', 
                                           args=name, 
                                           kwargs=dict(hi=True)))
                gevent.sleep(self.wait)
                job_out = dom.method_calls[-1][1][1]
                assert 'exc' in job_out
                assert job_out['exc'].startswith("TypeError('Job arguments must be a non-string")


