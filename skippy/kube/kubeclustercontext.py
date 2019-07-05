import logging

from kubernetes import client
from kubernetes.client import CoreV1Api
from core.clustercontext import ClusterContext
from core.model import Node, Pod
from kube.utils import create_nodes


class KubeClusterContext(ClusterContext):
    api: CoreV1Api
    nodes: [Node] = None

    def __init__(self: str):
        super().__init__()
        self.api = CoreV1Api()

    def place_pod_on_node(self, pod: Pod, node: Node):
        # Update the internal state of the super class
        super(KubeClusterContext, self).place_pod_on_node(pod, node)

        # But also actually create the placmement in kubernetes
        target = client.V1ObjectReference()
        target.kind = ''
        target.apiVersion = 'v1'
        target.name = node.name

        meta = client.V1ObjectMeta()
        meta.name = pod.name
        body = client.V1Binding(target=target, metadata=meta)
        logging.info('Creating namespaced binding: Pod %s on Node %s', pod.name, node.name)
        try:
            self.api.create_namespaced_binding(pod.namespace, body)
        except ValueError:
            # Due to a bug in the library, an error was thrown (but everything most likely worked fine).
            # https://github.com/kubernetes-client/python/issues/547
            pass

    def list_nodes(self):
        # TODO refresh node data but make sure to keep remaining capacities (allocatable)
        # Maybe implement a timeout to not request the data with every single pod placement?
        # Maybe also implement taking the images on the nodes from the node data (node.status.images.names contains
        # the tags, the sizes however aren't relevant as these are the unzipped ones)
        if self.nodes is None:
            self.nodes = create_nodes(self.api.list_node().items)
        return self.nodes
