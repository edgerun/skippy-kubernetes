import logging
from typing import List

from kubernetes import client
from kubernetes.client import CoreV1Api, V1Node
from core.clustercontext import ClusterContext
from core.model import Node, Capacity, Pod, ImageState
from core.utils import parse_size_string


class KubeClusterContext(ClusterContext):
    api: CoreV1Api
    nodes: [Node] = None

    def __init__(self, target_namespace: str):
        super().__init__()
        self.target_namespace = target_namespace
        self.api = CoreV1Api()

    @staticmethod
    def __create_node(v1node: V1Node) -> Node:
        name = v1node.metadata.name
        labels = v1node.metadata.labels
        cpu_millis = int(v1node.status.capacity['cpu']) * 1000
        memory = parse_size_string(v1node.status.capacity['memory'])
        capacity = Capacity(cpu_millis, memory)
        allocatable = Capacity(cpu_millis, memory)
        return Node(name=name, labels=labels, capacity=capacity, allocatable=allocatable)

    def __create_nodes(self, v1nodes: List[V1Node]) -> List[Node]:
        return [self.__create_node(node) for node in v1nodes]

    def place_pod_on_node(self, pod: Pod, node: Node):
        # Update the internal state of the super class
        super(KubeClusterContext, self).place_pod_on_node(pod, node)

        # But also actually create the placmement in kubernetes
        target = client.V1ObjectReference()
        target.kind = ""
        target.apiVersion = "v1"
        target.name = node.name

        meta = client.V1ObjectMeta()
        meta.name = pod.name
        body = client.V1Binding(target=target, metadata=meta)
        logging.info('Was about to send placement, but were only testing here...')
        # TODO revert self.api.create_namespaced_binding(self.target_namespace, body)

    def list_nodes(self):
        # TODO refresh node data but make sure to keep remaining capacities (allocatable)
        # Maybe implement a timeout to not request the data with every single pod placement?
        # Maybe also implement taking the images on the nodes from the node data (node.status.images.names contains
        # the tags, the sizes however aren't relevant as these are the unzipped ones)
        if self.nodes is None:
            self.nodes = self.__create_nodes(self.api.list_node().items)
        return self.nodes
