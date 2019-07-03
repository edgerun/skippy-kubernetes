import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class LivenessProbe:
    @staticmethod
    def serve_health(port=8080):
        class HealthRequestHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(b"Everything's lookin' good.")
                return
        httpd = HTTPServer(('', port), HealthRequestHandler)
        httpd.serve_forever()

    @staticmethod
    def start():
        daemon = threading.Thread(name='liveness_probe_server',
                                  target=LivenessProbe.serve_health)
        daemon.setDaemon(True)
        daemon.start()