import argparse
import ast
import json
import logging

from kubernetes import config, watch, client
from kubernetes.client.rest import ApiException

from skippy.core.priorities import BalancedResourcePriority, LatencyAwareImageLocalityPriority, LocalityTypePriority, \
    DataLocalityPriority, CapabilityPriority, ImageLocalityPriority
from skippy.core.scheduler import Scheduler
from skippy.kube.kubeclustercontext import KubeClusterContext
from skippy.kube.liveness_probe import LivenessProbe
from skippy.kube.utils import create_pod


def main():
    # Parse the arguments
    parser = argparse.ArgumentParser(description='Skippy - Navigating functions to the edge of the world (i.e. K8s)')
    parser.add_argument('-s', '--scheduler-name', action='store', dest='scheduler_name',
                        help='Change the name of the scheduler. New pods which should be placed by this scheduler need '
                             'to define this name. Set \'None\' to disable the name check (if this scheduler is '
                             'completely replacing the kube-scheduler).', default='skippy-scheduler')
    parser.add_argument('-n', '--namespace', action='store', dest='namespace',
                        help='Only watch pods of a specific namespace.')
    parser.add_argument('-w', '--weights', action='store', dest='weights',
                        help='An array of floats defining the weights of the different priority functions',
                        default=None)
    parser.add_argument('-u', '--use-default', action='store_true', dest='default',
                        help='Use the predicate and priority functions of the default scheduler.', default=False)
    parser.add_argument('-c', '--kube-config', action='store_true', dest='kube_config',
                        help='Load kube-config from home dir instead of in-cluster-config from envs.', default=False)
    parser.add_argument('-d', '--debug', action='store_true', dest='debug',
                        help='Enable debug logs.', default=False)
    args = parser.parse_args()
    level = logging.DEBUG if args.debug else logging.INFO
    scheduler_name = None if args.scheduler_name == 'None' else args.scheduler_name
    namespace = None if args.namespace == 'None' else args.namespace
    weights = None if args.weights is None else [float(x) for x in ast.literal_eval(args.weights)]

    # Set the log level
    logging.getLogger().setLevel(level)

    # Load the kubernetes API config
    if args.kube_config:
        # Load the configuration from ~/.kube
        logging.debug('Loading kube config...')
        config.load_kube_config()
    else:
        # Load the configuration when running inside the cluster (by reading envs set by k8s)
        logging.debug('Loading in-cluster config...')
        config.load_incluster_config()

    if weights:
        logging.info('Using custom weights: %s', weights)

    # Initialize the API, context and scheduler
    cluster_context = KubeClusterContext()
    api = client.CoreV1Api()
    if args.default:
        logging.debug('Using default scheduler priority functions')
        priorities = [(1.0, BalancedResourcePriority()),
                      (1.0, ImageLocalityPriority())] if weights is None \
            else [(weights[0], BalancedResourcePriority()),
                  (weights[1], ImageLocalityPriority())]
    else:
        priorities = None if weights is None \
            else [(weights[0], BalancedResourcePriority()),
                  (weights[1], LatencyAwareImageLocalityPriority()),
                  (weights[2], LocalityTypePriority()),
                  (weights[3], DataLocalityPriority()),
                  (weights[4], CapabilityPriority())]
    scheduler = Scheduler(cluster_context, priorities=priorities)

    # Either watch all namespaces or only the one set as argument
    w = watch.Watch()
    if namespace is not None:
        logging.debug('Watching for new pod events in namespace %s...', args.namespace)
        stream = w.stream(api.list_namespaced_pod, args.namespace)
    else:
        logging.debug('Watching for new pod events across all namespaces...')
        stream = w.stream(api.list_pod_for_all_namespaces)

    if scheduler_name:
        logging.debug('Watching for new pods with defined scheduler-name \'%s\'...', scheduler_name)
    else:
        logging.debug('Watching for pods without caring about defined scheduler-name...')

    # Start the liveness probe (used by kubernetes to restart the service if it's not responding anymore)
    logging.debug('Starting liveness / readiness probe...')
    LivenessProbe.start()

    # Main event loop watching for new pods
    logging.info('Everything is in place for new pods to be scheduled. Waiting for new events...')
    try:
        for event in stream:
            # noinspection PyBroadException
            try:
                if event['object'].status.phase == 'Pending' and \
                        (scheduler_name is None or event['object'].spec.scheduler_name == scheduler_name) and \
                        event['type'] == 'ADDED':
                    pod = create_pod(event['object'])
                    logging.debug('There\'s a new pod to schedule: ' + pod.name)
                    result = scheduler.schedule(pod)
                    logging.debug('Pod yielded %s', result)
            except ApiException as e:
                # Parse the JSON message body of the exception
                logging.exception('ApiExceptionMessage: %s', json.loads(e.body)['message'])
            except Exception:
                # We really don't want the scheduler to die, therefore we catch Exception here
                logging.exception('Exception in outer event loop caught. '
                                  'It will be ignored to make sure the scheduler continues to run.')
    except KeyboardInterrupt:
        logging.info('Shutting down after receiving a keyboard interrupt.')


if __name__ == '__main__':
    main()
