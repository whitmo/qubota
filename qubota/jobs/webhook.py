"""
Jobs that come included
"""
import requests.api
import logging



class WebHook(object):
    """
    A class of jobs for working webhooks

    """
    data_verbs = {'put', 'patch', 'post'}
    log = logging.getLogger(__name__)

    def __init__(self, uid, run):
        self.run = run
        self.uid = uid

    def make_request(self, action, **kw):
        verb, url = action.rsplit(' ', 1)
        verb = verb.lower()
        data = kw.pop('data')
        if not verb in self.data_verbs:
            assert not data 

        ctor = getattr(requests.api, verb)
        response = ctor(url, **kw)
        return response

    def load_handler_from_map(self, mapping, key, default=None):
        candidate = mapping.get(key, None)
        return self.load_handler(candidate, default)

    def load_handler(self, candidate, default=None):
        if candidate:
            try:
                return self.job.resolve(candidate)
            except ImportError:
                self.log.warn('loading %s failed' %candidate)

        if default is not None:
            if callable(default):
                return default
            return self.load_handler(default)
        

    def __call__(self, action, **kw):
        """
        - an action is specified "{http verb} {url}" 

        - kw are any arguments required to make a request or series of
          requests
        """
        return self.execute(action, **kw)
    

class OneShot(WebHook):
    """
    Send a payload to a single url
    """
    def execute(self, action, **kw):
        response = self.make_request(action, **kw)
        rh = self.load_handler_from_map('response_handler', 
                                        default=self.default_response_handler)
        return rh(response)
        # determine failure?!
        # log returned body?!



class DoubleShot(WebHook):
    """
    A two linked http requests and responses
    """

