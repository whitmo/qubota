from .resolver import resolve
from contextlib import contextmanager
from functools import partial
# from ginkgo.runner import ControlInterface
# from ginkgo.runner import resolve_pid
# from ginkgo.runner import setup_process
from path import path
import sys


def queue_and_domain(prefix=None):
    from qubota.cli import CLIApp
    app = CLIApp()
    if prefix is None:
        prefix = app.prefix
    return app.queue(prefix), app.domain(prefix)
    

@contextmanager
def log_tb(logger, raise_err=False):
    try:
        yield
    except Exception, e:
        logger.error(e, exc_info=True)
        if raise_err:
            raise

def readf(name, parent=None):
    if parent is None:
        frame = sys._getframe(2)
        flocals = frame.f_locals
        parent = path(flocals['__file__']).parent
    f = parent / name
    return f.text().strip()


class AttrAttr(object):
    """
    A descriptor for proxying an attribute of an attribute
    """
    def __init__(self, parent, attr):
        self.parent = parent
        self.attr = attr

    def __get__(self, obj, type=None):
        parent = getattr(obj, self.parent)
        return getattr(parent, self.attr)


app_attr = partial(AttrAttr, 'app')


class reify(object):
    #@@ from pyramid
    """ Use as a class method decorator.  It operates almost exactly like the
    Python ``@property`` decorator, but it puts the result of the method it
    decorates into the instance dict after the first call, effectively
    replacing the function it decorates with an instance variable.  It is, in
    Python parlance, a non-data descriptor.  An example:

    .. code-block:: python

       class Foo(object):
           @reify
           def jammy(self):
               print 'jammy called'
               return 1

    And usage of Foo:

    .. code-block:: text

       >>> f = Foo()
       >>> v = f.jammy
       'jammy called'
       >>> print v
       1
       >>> f.jammy
       1
       >>> # jammy func not called the second time; it replaced itself with 1
    """
    def __init__(self, wrapped):
        self.wrapped = wrapped
        try:
            self.__doc__ = wrapped.__doc__
        except: # pragma: no cover
            pass

    def __get__(self, inst, objtype=None):
        if inst is None:
            return self
        val = self.wrapped(inst)
        setattr(inst, self.wrapped.__name__, val)
        return val


@contextmanager
def app(spec, config=None):
    service = resolve(spec) 
    app = service(config=config)
    try:
        yield app
    finally:
        if app.state.current != 'stopping':
            app.stop()


def runner(target, help, error, stdout, daemonize, print_usage):
    stdout.write('\n')
    if target and help:
        try:
            app = setup_process(target)
            return app.config.print_help(stdout=stdout)
        except RuntimeError, e:
            return error(e)
        
    if target:
        try:
            ControlInterface().start(target, daemonize)
        except RuntimeError, e:
        #     pass
        # except Exception, e:
        #     import pdb, sys;pdb.post_mortem(sys.exc_info()[2])
            return error(e)

    return print_usage


def control(pid, target, error, action):
    if pid and target:
        error("You cannot specify both a target and a pid")

    try:
        if action in "start restart log logtail".split():
            if not target:
                error("You need to specify a target for {}".format(action))
            getattr(ControlInterface(), action)(target)
        else:
            getattr(ControlInterface(), action)(resolve_pid(pid, target))
    except RuntimeError, e:
        error(e)



