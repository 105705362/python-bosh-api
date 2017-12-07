import requests, time, json, io
from urllib.parse import urlparse

from json.decoder import WHITESPACE

def json_iterload(string_or_fp, cls=json.JSONDecoder, **kwargs):
    if isinstance(string_or_fp, io.IOBase):
        string = string_or_fp.read()
    else:
        string = str(string_or_fp)

    decoder = cls(**kwargs)
    idx = WHITESPACE.match(string, 0).end()
    while idx < len(string):
        obj, end = decoder.raw_decode(string, idx)
        yield obj
        idx = WHITESPACE.match(string, end).end()

class BoshError(Exception):
    pass

class BoshRequestError(BoshError):
    def __init__(self, method, endpoint, code, text):
        self.code = code
        self.text = text
        self.method = method
        self.endpoint = endpoint
    def __repr__(self):
        return "<BoshRequestError: %d - %s on %s %s> "%(self.code,
                                                        self.text, self.method, self.endpoint)

class BoshUaaError(BoshError):
    def __init__(self, code, text):
        self.code = code
        self.text = text
    def __repr__(self):
        return "<UaaError: %d - %s>"%(self.code, self.text)

class BoshObjError(BoshError):
    pass
class BoshObject():
    _keywords = ()
    _pk = None
    def __init__(self, data):
        f = [x for x in self._keywords if x not in data]
        if len(f) > 0:
            raise BoshObjError("%s is missing attr: %s"%(self.__class__, ",".join(f)))
        self._data = data
    def __getattr__(self, a):
        if a in self._data:
            return self._data[a]
        raise BoshObjError("%s does not have %s"%(self.__class__, a))
    def __repr__(self):
        if isinstance(self._pk, str):
            return "<%s %s=%s>"%(self.__class__.__name__, self._pk, repr(self._data.get(self._pk)))
        if isinstance(self._pk, tuple):
            return "<%s %s>"%(self.__class__.__name__,
                              ", ".join(["%s=%s"%(x, repr(self._data.get(x))) for x in self._pk]))
        return "<%s Generic>"%self.__class__.__name__

class BoshTask(BoshObject):
    _keywords = ('id', 'state', 'description', 'timestamp', 'started_at', 'result', 'user', 'deployment', 'context_id')
    _pk = ('id', 'state')
class BoshDeploymentInfo(BoshObject):
    _keywords = ('name', 'releases', 'stemcells', 'cloud_config', 'teams')
    _pk = 'name'
class BoshDeployment(BoshObject):
    _keywords = ('manifest',)
class BoshInstance(BoshObject):
    _keywords = ('agent_id', 'cid', 'job', 'index', 'id', 'az', 'ips', 'vm_created_at', 'expects_vm')
    _pk = ('job', 'index', 'id')
    
class UaaClient():
    token_service = '/oauth/token'
    access_token = None
    expires_in = 0
    payload = {"grant_type":"client_credentials"}
    def _strip_tailing_slash(self, u):
        if u[-1] == "/":
            return u[0:-1]
        else:
            return u
    def __init__(self, base_url, client_id, client_secret, verify=False):
        self.base_url = self._strip_tailing_slash(base_url)
        self.client_id = client_id
        self.client_secret = client_secret
        self.verify = verify
    def auth(self):
        r = requests.post(self.base_url + self.token_service,
                          verify = self.verify,
                          data = self.payload,
                          auth = (self.client_id, self.client_secret))
        if not r.status_code == 200:
            raise UaaError(r.status_code, r.text)
        t = json.loads(r.text)
        self.access_token = t["access_token"]
        self.expires_in = time.time() + t["expires_in"]
        return self.access_token
    def __call__(self, r):
        if self.expires_in < time.time()+5:
            self.auth()
        r.headers['Authorization'] = "Bearer %s"%self.access_token
        return r
                          
class BoshEnv():
    def __init__(self, director_ip, client, client_secret, cacert=False):
        self.uaa = UaaClient("https://%s:8443"%director_ip, client, client_secret, verify = cacert)
        self.env = "https://%s:25555"%director_ip
        self.verify = cacert

    def _dispatch(self, method, endpoint, param, data, **argv):
        url = "%s%s"%(self.env, endpoint)
        for k,v in argv.items():
            url = url.replace("<%s>"%k, str(v))
        with requests.Session() as s:
            s.verify = self.verify
            s.auth = self.uaa
            if isinstance(data, str) and method in ('PUT', 'POST', 'PATCH'):
                s.headers["Content-Type"] = "text/yaml"
            if isinstance(data, dict):
                s.headers["Content-Type"] = "text/json"
                data = json.dumps(data)
            resp = s.request(method, url, param, data, allow_redirects=False)
            if resp.status_code == 200:
                return [i for i in  json_iterload(resp.text)]
            if resp.status_code == 302:
                redir =  resp.headers["Location"]
                parsed = urlparse(redir)
                task = self._get(parsed.path, None, None)
                return task
            raise BoshRequestError(method, url, resp.status_code, resp.text)
                                       
    def __getattr__(self, attname):
        if attname[0] == '_' and attname[1:].upper() in ('GET','PUT','POST','DELETE','HEAD', 'PATCH'):
            def disp(endpoint,param,  data, **argv):
                return self._dispatch(attname[1:].upper(), endpoint, param, data, **argv)
            return disp
        raise BoshError("not supported method: %s"%attname)

    def tasks(self, **argv):
        """ GET /tasks
        arguments:
                       state = queued, processing, calcelling, done
                  deployment = deployment_name
        return: list of `BoshTask'
        """
        res = next(self._get("/tasks", param=argv, data=None))
        return [ BoshTask(t) for t in res ]

    def task_by_id(self, task_id):
        """ GET /tasks/<task_id>
        return: BoshTask
        """
        return BoshTask(next(self._get("/tasks/<task_id>",
                                       param=None,
                                       data=None, task_id = task_id)))

    def deploy(self, manifest, **param):
        """ POST /deployment
        params: 
                recreate = true
              skip_drain = job1,...
        return: BoshTask
        """
        return BoshTask(next(self._post("/deployments", param = param, data=manifest)))

    def deployments(self):
        """ GET /deployments
        return: list of  BoshDeploymentInfo
        """
        res = next(self._get("/deployments", param = None, data=None))
        return [ BoshDeploymentInfo(x) for x in res ]

    def deployment_by_name(self, deployment_name):
        """ GET /deployments/<deployment_name> 
        return: BoshDeployment
        """
        return BoshDeployment(next(self._get("/deployments/<deployment_name>", param=None,
                                             data=None,
                                             deployment_name=deployment_name)))

    def instances(self, deployment_name):
        """ GET /deployments/<deployment_name>/instances
        return list of BoshInstance
        """
        res = next(self._get("/deployments/<deployment_name>/instances", param=None,
                        data=None,
                        deployment_name=deployment_name))
        return [ BoshInstance(i) for i in res ]
    def run_errand(self, deployment_name, errand_name, **args):
        """ POST /deployments/<deployment_name>/errands/<errand_name>/runs
        arguments: 
                  keep_alive = true|false
                when_changed = true|false
                   instances = [] ;; list of BoshInstance
        return: BoshTask
        """
        if "instances" in args:
            args["instance"] = [x._data for x in args["instances"]]
        return BoshTask(next(self._post("/deployments/<deployment_name>/errands/<errand_name>/runs",
                                   param=None,
                                   data=args,
                                   deployment_name = deployment_name,
                                   errand_name = errand_name)))
        
