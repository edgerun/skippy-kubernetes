from typing import List

from kubernetes import client
from kubernetes.client import CoreV1Api, V1Node
from core.clustercontext import ClusterContext
from core.model import Node, Capacity, Pod, ImageState


class KubeClusterContext(ClusterContext):

    api: CoreV1Api

    def __init__(self, target_namespace: str):
        self.target_namespace = target_namespace
        self.api = CoreV1Api()

    def __create_capacity(self, v1node: V1Node) -> Capacity:
        # TODO create Capacity from Node Status
        # capacity = Capacity(v1node.status[''])
        # return capacity
        pass

    @staticmethod
    def __create_node(v1node: V1Node) -> Node:
        #  self, name: str, capacity: Capacity = None, allocatable: Capacity = None,
        #  labels: Dict[str, str] = None) -> None:
        # TODO create Node from V1Node
        return Node(v1node.metadata.name)

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
        self.api.create_namespaced_binding(self.target_namespace, body)

    def retrieve_image_state(self, image_name: str) -> ImageState:
        # TODO use a docker hub api to get the image size of the requested docker image
        pass

    def list_nodes(self):
        return self.__create_nodes(self.api.list_node().items)
