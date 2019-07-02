import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from kubernetes import config, watch, client
from kubernetes.client import V1Event, V1Pod, V1Container
from kubernetes.client.rest import ApiException

from core.model import Pod, PodSpec, Container, ResourceRequirements
from core.scheduler import Scheduler
from core.utils import parse_size_string
from kube.kubeclustercontext import KubeClusterContext


watched_namespace = 'openfaas-fn'
#scheduler_name = 'skippy'
inside_cluster = True
scheduler_name = None
#inside_cluster = False


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


def create_container(container: V1Container) -> Container:
    name = container.image
    resources = None
    if container.resources.requests is not None:
        resources = ResourceRequirements()
        resources.requests = dict(zip(container.resources.requests.keys(), [parse_size_string(value)
                                                                          for value in
                                                                          container.resources.requests.values()]))
    return Container(name, resources)


def create_pod(pod: V1Pod):
    name = pod.metadata.name
    containers = [create_container(c) for c in pod.spec.containers]
    spec: PodSpec = PodSpec(containers)
    return Pod(name, spec)


def main():
    # TODO parse
    #  - scheduler name
    #  - if the scheduler should read the in-cluster-config
    #  - the namespace to watch or refactor usage to get rid of namespaces
    #    (and take namespace from deployment's podSpec)
    #    f.e. api.list_pod_for_all_namespaces()
    # TODO check if the scheduler should be completely replaced or openfaas should set the scheduler-name
    #   - This would imply changes in https://github.com/openfaas/faas-netes/blob/master/handlers/deploy.go#L161
    logging.getLogger().setLevel(logging.DEBUG)

    if inside_cluster:
        # Load the configuration when running inside the cluster
        logging.info('Loading incluster config...')
        config.load_incluster_config()
    else:
        logging.info('Loading kube config...')
        config.load_kube_config()

    cluster_context = KubeClusterContext(watched_namespace)
    api = client.CoreV1Api()
    scheduler = Scheduler(cluster_context)

    logging.info('Starting liveness probe...')
    start_liveness_probe()

    logging.info('Watching for new pod events...')
    w = watch.Watch()
    for event in w.stream(api.list_namespaced_pod, watched_namespace):
        # noinspection PyBroadException
        try:
            if event['object'].status.phase == "Pending" and \
                    (scheduler_name is None or event['object'].spec.scheduler_name == scheduler_name) and \
                    event['type'] == 'ADDED':
                pod = create_pod(event['object'])
                logging.info('There\'s a new pod to schedule: ' + pod.name)
                result = scheduler.schedule(pod)
                logging.debug('Pod yielded %s', result)
        except ApiException as e:
            logging.exception('ApiExceptionMessage: %s', json.loads(e.body)['message'])
        except ValueError:
            # Due to a bug in the library, an error was thrown (but everything most likely worked fine).
            # https://github.com/kubernetes-client/python/issues/547
            logging.exception('ValueError in outer event loop caught. '
                              'This could be caused by https://github.com/kubernetes-client/python/issues/547.')
        except Exception:
            logging.exception('Exception in outer event loop caught. '
                              'It will be ignored to make sure the scheduler continues to run.')


if __name__ == '__main__':
    main()
