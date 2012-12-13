from . import ctx
from fabric import api as fab
from fabric import network
from fabric.contrib import files
from mock import Mock
from mock import patch
import gevent
import logging
import os
import sys
import time
import uuid


log = logging.getLogger(__name__)


def poll_state(inst, target='running'):
    while inst.state != target:
        gevent.sleep(0.5)
    return True

def poll_ssh(config):
    with config:
        out = None
        while out is None:
            gevent.sleep(5)
            try:
                with fab.settings(warn_only=True):
                    out = fab.run('echo "Is it up?"')
            except network.NetworkError:
                pass
    return out


def poll_file(fp, config):
    with config:
        while not files.exists(fp):
            gevent.sleep(5)
    return True


def setup(): # wierdness with passing in ctx
    from qubota.cli import CLIApp
    from fabric import api

    ctx.app = app = CLIApp()
    ctx.fab = api
    ctx.keyfile = os.environ['SSH_KEYFILE']
    ctx.up = False

    start = time.time()
    node = ctx.node = boxutils.makenode(app)

    gr = gevent.spawn_later(10, poll_state, node)
    with gevent.Timeout(60*5, False):
        gr.join()
    
    if not gr.value is True:
        log.error("instance slow to come up: %s" %node.public_dns_name)
        node.terminate()
        sys.exit(1)
    ctx.file_timeout = 60*2
    ctx.last_peak_time = 0
    ctx.config = lambda : fab.settings(host_string=node.public_dns_name, 
                                       key_filename=ctx.keyfile, 
                                       abort_on_prompts=True,
                                       user='ec2-user',
                                       warn_only=True)    

    with gevent.Timeout(60*2, False):
        gr = gevent.spawn_later(0.1, poll_ssh, ctx.config())
        gr.join()

    if gr.value is None:
        sys.exit(1)

    log.info("spun up in %ss" %(time.time() - start))


clilog = '/var/log/cloud-init.log'


class boxutils(object):
    """
    Wherein we inspect whether the cloud init worked
    """
    ctx = ctx

    @staticmethod
    def makenode(app):
        from qubota.cli import QUp
        ctx.app_args = Mock()
        cmd = QUp(app, ctx.app_args)
        uid = str(uuid.uuid4())[:6]
        name = "qubota.functest.{}:{}".format(__name__, uid)
        return cmd.up_node(name)

    files_to_test = [
        '/tmp/req.txt',
        '/etc/init/qb-circusd.conf',
        '/home/ec2-user/app/qubota/etc/circus.ini',
        '/home/ec2-user/app/postactivate',
        '/home/ec2-user/app/qubota/bin/activate', #fix
        '/home/ec2-user/app/qubota/bin/qb',
        '/home/ec2-user/app/qubota/bin/circusd',
        clilog]

    @staticmethod
    def file_exists(f):
        #script leaves a semaphore now
        found = None
        start = time.time()
        with gevent.Timeout(ctx.file_timeout, False):
            found = poll_file(f, ctx.config())
        ctx.last_peak_time = start - time.time()
        if found is None:
            ctx.file_timeout = 10 

        assert found, f


def test_cloudinit_files():
    """
    workout cloud init
    """
    if boxutils.file_exists('/tmp/mkenv-done'):
        print "finished cloud-init setup in less than %s" %ctx.last_peak_time 

    with patch('qubota.cli.CLIApp.prefix', new='qubota.test'):
        for f in boxutils.files_to_test:
            yield boxutils.file_exists, f
            print "found in %s" %ctx.last_peak_time 

def teardown():
    with ctx.config():
        for f in fab.get(clilog, './cli-log.txt'):
            #print(path(f).text())
            pass
    log.info('terminate node')
    #ctx.node.terminate()
    del ctx.node
    del ctx.app

