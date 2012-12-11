import os
from fabric import network as net
from stuf import stuf


ctx = context = stuf()


def setup(ctx=ctx):
    pass

def teardown():
    net.disconnect_all()


