from .patch_requests import install_opener
from .patch_requests import uninstall_opener
from mock import Mock
from mock import patch
from webob.dec import wsgify
from webob import Response
import unittest
import wsgi_intercept


def setup():
    print "setup"
    install_opener()

def teardown():
    print "teardown"
    uninstall_opener()


class TestWebhook(unittest.TestCase):

    def the_response(self, request):
        return Response()

    @wsgify
    def target(self, request):
        self.current_request = request
        res = self.the_response(request)
        return res

    def make_one(self, uid='abcd1', job=None):
        if job is None:
            job = Mock()
        # if queue is None:
        #     queue = Mock()
        # if parent is None:
        #     parent = Mock()
        # from qubota.drain import JobRun
        from qubota.jobs.webhook import WebHook
        run = Mock()
        return WebHook(uid, run)
    
    def test_webhook_basic_GET(self):
        wsgi_intercept.add_wsgi_intercept('target', 80, lambda : self.target)
        self.make_one()("GET http://target:80")
        assert self.current_request.url == 'http://target/'
        assert self.current_request.host_port == '80'

    def test_webhook_GET_w_data(self):
        wsgi_intercept.add_wsgi_intercept('target', 80, lambda : self.target)
        self.make_one()("GET http://target:80", data=dict(hi='there'))
        assert 'hi' in self.current_request.GET

    def test_webhook_PUT_w_data(self):
        wsgi_intercept.add_wsgi_intercept('target', 80, lambda : self.target)
        self.make_one()("PUT http://target:80", data=dict(hi='there'))
        assert self.current_request.body == '{"hi": "there"}'
        



class TestPipeline(unittest.TestCase):
    """
    """    

