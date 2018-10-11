from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib import parse
import json


class Handler(BaseHTTPRequestHandler):
    dispatchers = {}

    def _set_headers(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Request-Method", "*")
        self.end_headers()

    @classmethod
    def Register(cls, path, fun):
        cls.dispatchers[path] = fun
        print('Registered service {}:{}'.format(path, fun.__name__))

    def do_GET(self):
        self.log_message("in get method")
        o = parse.urlparse(self.path)
        params_dict = parse.parse_qs(o.query)
        if o.path in self.dispatchers:
            fun = self.dispatchers[o.path]
            response = fun(params_dict)
        else:
            bad_request = 'error url path: {}'.format(self.path)
            res_json = {}
            res_json['code'] = 400
            res_json['error'] = bad_request
            response = json.dumps(res_json, ensure_ascii=True)
        self._set_headers()
        self.wfile.write(response.encode('utf-8'))


    def do_POST(self):
        self.log_message("in post method")
        post_data = self.rfile.read(int(self.headers['Content-Length']))
        body_params = json.loads(post_data.decode('utf-8'))
        o = parse.urlparse(self.path)
        params_dict = parse.parse_qs(o.query)
        params_dict.update(body_params)
        if o.path in self.dispatchers:
            fun = self.dispatchers[o.path]
            response = fun(params_dict)
        else:
            bad_request = 'error url path: {}'.format(self.path)
            res_json = {}
            res_json['code'] = 400
            res_json['error'] = bad_request
            response = json.dumps(res_json, ensure_ascii=True)
        self._set_headers()
        self.wfile.write(response.encode('utf-8'))


class MyHTTPServer(HTTPServer):

    def __init__(self, host, port):
        self.server = super(MyHTTPServer, self).__init__((host, port), Handler)

    def Register(self, path, fun):
        Handler.Register(path, fun)