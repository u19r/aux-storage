"""Controlled HTTP boundary for the ignored full-provider allocation comparison."""

import http.server
import hashlib
import json
import socket
import sys

config = json.loads(sys.stdin.readline())
expected = config["request"].encode()
response = config["response"].encode()
attempts = 0
errors = []


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def log_message(self, *_args):
        pass

    def do_POST(self):
        global attempts
        body = self.rfile.read(int(self.headers["Content-Length"]))
        attempts += 1
        if body != expected:
            errors.append("request bytes differ")
        if self.headers.get("x-amz-target") != config["target"]:
            errors.append("operation header differs")
        if self.headers.get("Content-Type") != "application/x-amz-json-1.0":
            errors.append("content type differs")
        if config["signed"]:
            if self.headers.get("x-amz-content-sha256") != hashlib.sha256(body).hexdigest():
                errors.append("signed payload checksum differs")
            if not self.headers.get("Authorization", "").startswith("AWS4-HMAC-SHA256 "):
                errors.append("signature missing")
            if not self.headers.get("x-amz-date"):
                errors.append("signing date missing")
        elif self.headers.get("Authorization"):
            errors.append("unexpected signature")
        retry = config["retry"] and attempts % 2 == 1
        body = b'{"__type":"ServiceUnavailable","message":"retry"}' if retry else response
        self.send_response(503 if retry else 200)
        self.send_header("Content-Type", "application/x-amz-json-1.0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        body = json.dumps({"attempts": attempts, "errors": errors}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)


server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
print(server.server_port, flush=True)
server.serve_forever()
