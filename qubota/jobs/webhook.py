import json
import logging
import requests.api
import urllib

class WebHookHTTPFailure(RuntimeError):
    """
    An unexpected http failure
    """

class WebHook(object):
    """
    A class of jobs for working webhooks
    """
    data_verbs = {'put', 'patch', 'post'}
    log = logging.getLogger(__name__)
    exception = WebHookHTTPFailure

    def __init__(self, uid, run):
        self.run = run
        self.uid = uid

    def make_request(self, action, **kw):
        verb, url = action.rsplit(' ', 1)
        verb = verb.lower()

        data = kw.pop('data', None)
        if not verb in self.data_verbs:
            if data is not None:
                data = urllib.urlencode(data)
                url = "{}?{}".format(url, data)
        else:
            if data is not None:
                if isinstance(data, dict) or isinstance(data, list):
                    data = json.dumps(data)
                kw['data'] = data

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

    @staticmethod
    def default_response_handler(webhook, response):
        if not response.ok:
            raise webhook.exception("%d %s" %(response.status_code, response.reason))
        return response
    
    def execute(self, action, **kw):
        response = self.make_request(action, **kw)
        rh = self.load_handler_from_map(kw, 'response_handler', 
                                        default=self.default_response_handler)
        #if rh is not None:
        assert rh, 'No response handler: %s' %kw
        res = rh(self, response)    
        return res


class HookPipeline(WebHook):
    """
    A two linked http requests and responses
    """

    def pipe(self, step, action, data, **kw):
        if action is None:
            action = data.pop('action', None)
        if action is None:
            raise ValueError('No action given')

        kw.update(data)
        response = WebHook.execute(self, action, **kw)
        return response.json()

    def execute(self, actions, **kwargs):
        data = {}
        for step, action in enumerate(actions):
            kw = kwargs.get(action, {}) 
            data = self.pipe(step, action, data, **kw)




