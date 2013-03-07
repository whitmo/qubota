from ..resolver import resolve
import json
import logging
import requests
import time
import urllib
import pprint


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
    resolve = staticmethod(resolve)

    def __init__(self, uid, run):
        self.run = run
        self.uid = uid
        self.run.parent.local_cache.requests = requests.Session()
        self.requests = self.run.parent.local_cache.requests

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

        ctor = getattr(self.requests, verb)
        response = ctor(url, **kw)
        return response

    def load_handler_from_map(self, mapping, key, default=None):
        candidate = mapping.pop(key, None)
        return self.load_handler(candidate, default)

    def load_handler(self, candidate, default=None):
        if candidate:
            try:
                return resolve(candidate)
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
        rh = self.load_handler_from_map(kw, 'response_handler', 
                                        default=self.default_response_handler)
        response = self.make_request(action, **kw)
        assert rh, 'No response handler: %s' %kw
        res = rh(self, response)    
        return res


class HookPipeline(WebHook):
    """
    A two linked http requests and responses
    """

    @staticmethod
    def default_response_handler(webhook, response):
        if not response.ok:
            raise webhook.exception("%d %s" %(response.status_code, response.reason))
        resjs = response.json()
        return resjs

    def pipe(self, step, action, data, extra):
        if action is None:
            action = data.pop('action', None)

        if action is None:
            raise ValueError('No action given')
        
        extra.update(data)

        self.log.info("%s %s: %s" %(step, action, pprint.pformat(extra)))
        resjs = WebHook.execute(self, action, **extra)
        return resjs

    @staticmethod
    def merge(first, second):
        out = first.copy()
        out.update(second)
        return out

    def execute(self, actions, **kwargs):
        data = {}
        default_kw = kwargs.pop('default', {})
        for step, action in enumerate(actions):
            kw = self.merge(default_kw, kwargs.get(action, {}))
            data = self.pipe(step, action, data, kw)
            self.log.info("END STEP (%s): %s", step, pprint.pformat(data))
        return data



def add_extra_plrh(hook, res, ukey='qubota.uid', tkey='qubota.pipeine.time', 
                   rhbase=HookPipeline.default_response_handler):
    resjs = rhbase(hook, res)
    data = resjs.setdefault('data', {})
    data[ukey] = hook.uid
    data.setdefault(tkey, {})[res.request.url] = time.time()
    return resjs

