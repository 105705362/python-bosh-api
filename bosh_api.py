import requests, time, json

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
    def __init__(self, base_url, client_id, client_secret, verify=None):
        self.base_url = self._strip_tailing_slash(base_url)
        self.client_id = client_id
        self.client_secret = client_secret
        self.verify = verify
    def auth(self):
        r = requests.post(self.base_url + token_service,
                          verify = self.verify,
                          data = self.payload,
                          auth = (self.client_id, self.client_secret))
        if not r.status_code == 200:
            raise UaaError(r.status_code, r.text)
        t = json.loads(r.text)
        self.access_token = t["access_token"]
        self.expires_in = time.time() + t["expires_in"]
        return access_token
    def __call__(self, r):
        if self.expires_in < time.time()+5:
            self.auth()
        r.headers['Authorization'] = "Bearer %s"%self.access_token
        return r
                          
class BoshEnv():
    def __init__(self, director_ip, client, client_secret, cacert=None):
        self.uaa = UaaClient("https://%s:8443"%director_ip, client, client_secret, verify = cacert)
        self.env = "https://%s:25555"%director_ip
        self.s = requests.Session()
        self.s.verify = cacert
        self.s.auth = self.uaa
    def _dispatch(self, method, endpoint, data, **argv):
        url = "%s%s"%(self.env, endpoint)
        for k,v in argv.items:
            url.replace("<%s>"%k, v)
        return self.s.request(method, url, data)

    def __getattr__(self, attname):
        if attname[0] == '_' and attname[1:].upper() in ('GET','PUT','POST','DELETE','HEAD', 'PATCH'):
            def disp(self, endpoint, data, **argv):
                return self._dispatch(attname[1:].upper(), endpoint, data, **argv)
            return disp
        raise BoshError("not supported method: %s"%attname)
    def tasks(self, **argv):
        resp = self._get("/tasks", data=argv)
        if resp.status_code != 200:
            raise BoshRequestError("GET", "/tasks", resp.status_code, resq.text)
        return json.loads(resp.text)
    def task_by_id(self, task_id, **argv):
        resp = self._get("tasks/<task_id>", argv, task_id = task_id)
        if resp.status_code != 200:
            raise BoshRequestError("GET", "/tasks", resp.status_code, resq.text)
        return json.loads(resp.text)
    def deploy(self, manifest):
        pass
    def deployments(self, **args):
        pass
    def deployment_by_name(self, deployment_name, **argv):
        pass
    def instances(self, deployment_name, **argv):
        pass
    def run_errand(self, deployment_name, errand_name, **argv):
        pass
