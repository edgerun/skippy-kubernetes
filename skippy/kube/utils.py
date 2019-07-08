from typing import List

from kubernetes.client import V1Pod, V1Container, V1Node

from core.model import Pod, Container, ResourceRequirements, PodSpec, Node, Capacity
from core.utils import parse_size_string


def create_nodes(v1nodes: List[V1Node]) -> List[Node]:
    return [create_node(node) for node in v1nodes]


def create_node(v1node: V1Node) -> Node:
    name = v1node.metadata.name
    labels = v1node.metadata.labels
    cpu_millis = int(v1node.status.capacity['cpu']) * 1000
    memory = parse_size_string(v1node.status.capacity['memory'])
    capacity = Capacity(cpu_millis, memory)
    allocatable = Capacity(cpu_millis, memory)
    return Node(name=name, labels=labels, capacity=capacity, allocatable=allocatable)


def create_container(container: V1Container) -> Container:
    name = container.image
    resources = None
    if container.resources.requests is not None:
        resources = ResourceRequirements()
        resources.requests = dict(zip(container.resources.requests.keys(), [parse_size_string(value)
                                                                            for value in
                                                                            container.resources.requests.values()]))
    return Container(name, resources)


def create_pod(pod: V1Pod) -> Pod:
    name = pod.metadata.name
    containers = [create_container(c) for c in pod.spec.containers]
    labels = pod.metadata.labels
    namespace = pod.metadata.namespace
    spec: PodSpec = PodSpec(containers, labels)
    return Pod(name, namespace, spec)
