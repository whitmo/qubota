from contextlib import contextmanager
from .resolver import resolve
from .utils import reify
from stuf import frozenstuf
from stuf import stuf
import logging
import os


class AbstractAsyncManager(object):
    """
    borrowed from ginkgo
    """
    def spawn(self, func, *args, **kwargs):
        raise NotImplementedError()

    def spawn_later(self, seconds, func, *args, **kwargs):
        raise NotImplementedError()

    def sleep(self, seconds):
        raise NotImplementedError()

    def queue(self, *args, **kwargs):
        raise NotImplementedError()

    def event(self, *args, **kwargs):
        raise NotImplementedError()

    def lock(self, *args, **kwargs):
        raise NotImplementedError()


class Config(dict):
    """
    A data structure for configuration information
    """
    load = stuf.update


class SettingInfo(frozenstuf):
    def to_dict(self):
        return dict((key, value.default) for key, value in self.items())

    def load(self, config={}):
        config.update(self.to_dict())
        return config


def match_descriptors(klass, descriptor_class):
    for name, inst in klass.__dict__.items():
        if isinstance(inst, descriptor_class):
            yield name, inst


class Setting(object):
    def __init__(self, default=None, help=None, ctor=None):
        self.default = default
        self.help = help
        self.name = None
        self.ctor = ctor

    def set_name(self, name):
        self.name = name

    def __get__(self, obj, objtype=None):
        if self.name is None:
            raise RuntimeError("name not initialized")
        return obj.config[self.name]
    
    def __set__(self, obj, value):
        raise ValueError("Read only value")

    @classmethod
    def initialize_all(cls, klass, extractor=match_descriptors, info_ctor=SettingInfo, 
                       attr='_defaults'):
        settings  = {name: setting for name, setting in extractor(klass, cls)}
        # annotate the klass
        setattr(klass, attr, info_ctor(settings)) 

        # set the name for each of the descriptors
        # since this is the first time we know it
        for name, setting in settings.items():
            setting.set_name(name)

        return klass





class Service(object):
    """
    A base class for services

    mostly borrowed from ginkgo, simplified
    """
    log = logging.getLogger(__name__)
    resolve = staticmethod(resolve)
    async_handler = None
    wait_interval = 0.01
    start_timeout = 0.5

    def __init__(self, config):
        if isinstance(config, dict):
            self.config.update(config.items())
        self.root_pid = os.getpid()        
        self.state = 'init'
        self.async = self.resolve(self.async_handler)()

    @reify
    def config(self):
        return Config(self._defaults.to_dict())

    def wait(self, state, timeout=None):
        if timeout is not None:
            while self.state != state:
                self.async.sleep(self.wait_interval)
        else:
            with self.async.timeout(timeout):
                while self.state != state:
                    self.async.sleep(self.wait_interval)

    def start(self, block_until_ready=True):
        """Starts children and then this service. By default it blocks until ready."""
        self.state = "start"
        ready = not self.do_start()
        if not ready and block_until_ready is True:
            self.wait("ready", self.start_timeout)
        self.state = "ready"

    up = {'starting', 'ready'}
    down = {"init", "stopped"}

    @property
    def running(self):
        return self.state in self.up

    def pre_stop(self):
        pass

    def post_stop(self):
        pass

    def do_stop(self):
        """Empty implementation of service stop. Implement me!"""
        return 

    def pre_start(self):
        pass

    def do_start(self):
        """Empty implementation of service start. Implement me!

        Return `service.NOT_READY` to block until :meth:`set_ready` is
        called (or `ready_timeout` is reached).

        """
        return

    def post_start(self):
        pass


    def stop(self):
        """Stop child services in reverse order and then this service"""
        if self.state in self.down:
            return
        ready_before_stop = self.ready
        self.state = "stop"
        if ready_before_stop:
            self.do_stop()
        self.state = "stopped"

    @property
    def ready(self):
        return self.state == 'ready'

    def main_loop(self):
        pass

    def serve_forever(self):
        """
        Start the service if it hasn't been already started and wait
        until it's stopped.
        """
        try:
            self.start()
        except RuntimeWarning:
            # If it can't start because it's
            # already started, just move on
            pass
        self.wait("stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()


@contextmanager
def app(spec, config=None):
    service = resolve(spec) 
    app = service(config=config)
    try:
        yield app
    finally:
        if app.state != 'stopping':
            app.stop()







