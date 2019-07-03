import json
import logging
import argparse

from kubernetes import config, watch, client
from kubernetes.client.rest import ApiException

from core.scheduler import Scheduler
from kube.kubeclustercontext import KubeClusterContext
from kube.liveness_probe import LivenessProbe
from kube.utils import create_pod


def main():
    # TODO check if the scheduler should be completely replaced or openfaas should set the scheduler-name
    #   - This would imply changes in https://github.com/openfaas/faas-netes/blob/master/handlers/deploy.go#L161

    # Set the log level
    logging.getLogger().setLevel(logging.DEBUG)

    # Parse the arguments
    parser = argparse.ArgumentParser(description='Skippy - Navigating functions to the edge of the world (i.e. K8s)')
    parser.add_argument('-s', '--scheduler-name',
                        action='store', dest='scheduler_name',
                        help='Change the name of the scheduler. New pods which should be placed by this scheduler need '
                             'to define this name. Set \'None\' to disable the name check (if this scheduler is '
                             'completely replacing the kube-scheduler).', default='skippy-scheduler')
    parser.add_argument('-n', '--namespace',
                        action='store', dest='namespace',
                        help='Only watch pods of a specific namespace.')
    parser.add_argument('-c', '--kube-config',
                        action='store_true', dest="kube_config",
                        help="Load kube-config from home dir instead of in-cluster-config from envs.", default=False)
    args = parser.parse_args()

    # Handle the edge-case that the scheduler-name should not be checked
    scheduler_name = None if args.scheduler_name == 'None' else args.scheduler_name

    # Load the kubernetes API config
    if args.kube_config:
        # Load the configuration from ~/.kube
        logging.info('Loading kube config...')
        config.load_kube_config()
    else:
        # Load the configuration when running inside the cluster (by reading envs set by k8s)
        logging.info('Loading in-cluster config...')
        config.load_incluster_config()

    # Initialize the API, context and scheduler
    cluster_context = KubeClusterContext()
    api = client.CoreV1Api()
    scheduler = Scheduler(cluster_context)

    # Start the liveness probe (used by kubernetes to restart the service if it's not responding anymore)
    logging.info('Starting liveness probe...')
    LivenessProbe.start()

    # Either watch all namespaces or only the one set as argument
    w = watch.Watch()
    if args.namespace is not None:
        logging.info('Watching for new pod events in namespace %s...', args.namespace)
        stream = w.stream(api.list_namespaced_pod, args.namespace)
    else:
        logging.info('Watching for new pod events across all namespaces...')
        stream = w.stream(api.list_pod_for_all_namespaces)

    # Main event loop watching for new pods
    for event in stream:
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
            # Parse the JSON message body of the exception
            logging.exception('ApiExceptionMessage: %s', json.loads(e.body)['message'])
        except ValueError:
            # Due to a bug in the library, an error was thrown (but everything most likely worked fine).
            # https://github.com/kubernetes-client/python/issues/547
            logging.exception('ValueError in outer event loop caught. '
                              'This could be caused by https://github.com/kubernetes-client/python/issues/547.')
        except Exception:
            # We really don't want the scheduler to die, therefore we catch Exception here
            logging.exception('Exception in outer event loop caught. '
                              'It will be ignored to make sure the scheduler continues to run.')


if __name__ == '__main__':
    main()
