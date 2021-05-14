import numpy as np
from typing import List, Union
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

data = {'result': 'this is a test'}
host = ('localhost', 8888)
cur_dir = './'


class Resquest(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.path="/index.html"
        try:
            # Check the file extension required and
            # set the right mime type

            sendReply = False
            if self.path.endswith(".html"):
                mimetype = 'text/html'
                cur_dir = './html/'
                sendReply = True
            if self.path.endswith("json"):
                mimetype = 'application/json'
                sendReply = True
            if self.path.endswith(".jpg"):
                mimetype = 'image/jpg'
                sendReply = True
            if self.path.endswith(".gif"):
                mimetype = 'image/gif'
                cur_dir = './picture/'
                sendReply = True
            if self.path.endswith(".js"):
                mimetype = 'application/javascript'
                sendReply = True
            if self.path.endswith(".css"):
                mimetype = 'text/css'
                sendReply = True

            if sendReply == True:
                self.send_response(200)
                self.send_header('Content-type', mimetype)
                self.end_headers()
                if mimetype == 'application/json':
                    self.wfile.write(json.dumps(data).encode())
                else:
                    f = open(cur_dir + self.path)
                    self.wfile.write(f.read())
                    f.close()
            return
        except IOError:
            self.send_error(404,'File Not Found: %s' % self.path)


if __name__ == '__main__':
    server = HTTPServer(host, Resquest)
    print("Starting server, listen at: %s:%s" % host)
    server.serve_forever()

