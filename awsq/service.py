from ginkgo.runner import ControlInterface
from ginkgo.runner import setup_process
from ginkgo.runner import resolve_pid


def runner(target, error, print_usage, daemonize=False):
    if target:
        print # blank line
        try:
            app = setup_process(target)
            app.config.print_help()
        except RuntimeError, e:
            return error(e)
        
    if target:
        try:
            ControlInterface().start(target, daemonize)
        except RuntimeError, e:
            return error(e)

    return print_usage()


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
