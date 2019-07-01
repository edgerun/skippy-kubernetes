import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from kubernetes import config, watch, client
from kubernetes.client import V1Event
from kubernetes.client.rest import ApiException

from core.model import Pod, PodSpec
from core.scheduler import Scheduler
from kube.kubeclustercontext import KubeClusterContext


watched_namespace = 'openfaas-fn'
scheduler_name = 'skippy'


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


def start_liveness_probe():
    daemon = threading.Thread(name='liveness_probe_server',
                              target=serve_health)
    daemon.setDaemon(True)
    daemon.start()


def execute_placement(api, pod_name, node_name, namespace=watched_namespace):
    target = client.V1ObjectReference()
    target.kind = ""
    target.apiVersion = "v1"
    target.name = node_name

    meta = client.V1ObjectMeta()
    meta.name = pod_name
    body = client.V1Binding(target=target, metadata=meta)

    print("Placing " + pod_name + " on node " + node_name)
    return api.create_namespaced_binding(namespace, body)


def create_pod(event: V1Event):
    # TODO Convert V1Event to Pod
    name = ''
    spec: PodSpec = PodSpec()
    return Pod(name, spec)


def main():
    logging.getLogger().setLevel(logging.DEBUG)

    # Load the configuration when running inside the cluster
    config.load_incluster_config()

    cluster_context = KubeClusterContext(watched_namespace)
    api = client.CoreV1Api()
    scheduler = Scheduler(cluster_context)

    start_liveness_probe()

    w = watch.Watch()
    for event in w.stream(api.list_namespaced_pod, watched_namespace):
        # noinspection PyBroadException
        try:
            if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == scheduler_name and \
                    event['type'] == 'ADDED':
                pod = create_pod(event)
                scheduler.schedule(pod)
        except ApiException as e:
            logging.exception('ApiExceptionMessage: %s', json.loads(e.body)['message'])
        except ValueError:
            # Due to a bug in the library, an error was thrown (but everything most likely worked fine).
            # https://github.com/kubernetes-client/python/issues/547
            pass
        except Exception:
            logging.exception('Exception in outer event loop caught. '
                              'It will be ignored to make sure the scheduler continues to run.')


if __name__ == '__main__':
    main()
