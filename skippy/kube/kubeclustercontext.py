import logging
from typing import Dict

from kubernetes import client
from kubernetes.client import CoreV1Api

from skippy.core.clustercontext import ClusterContext
from skippy.core.model import Node, Pod, ImageState
from skippy.kube.utils import create_nodes


class KubeClusterContext(ClusterContext):

    api: CoreV1Api
    nodes: [Node] = None
    storage_node: Node

    def __init__(self: str):
        super().__init__()
        self.api = CoreV1Api()

    def get_next_storage_node(self, node: Node) -> str:
        # TODO find the nearest one (instead of assuming there's only one)
        return self.storage_node.name

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

    def remove_pod_from_node(self, pod: Pod, node: Node):
        raise NotImplementedError

    def remove_pod_images_from_node(self, pod: Pod, node: Node):
        raise NotImplementedError

    def list_nodes(self):
        # TODO refresh node data but make sure to keep remaining capacities (allocatable)
        # Maybe implement a timeout to not request the data with every single pod placement?
        # Maybe also implement taking the images on the nodes from the node data (node.status.images.names contains
        # the tags, the sizes however aren't relevant as these are the unzipped ones)
        if self.nodes is None:
            self.nodes = create_nodes(self.api.list_node().items)
            self.storage_node = next(node for node in self.nodes if 'data.skippy.io/storage-node' in node.labels)
            # Filter nodes which should not be considered by Skippy for scheduling (but may contain data)
            self.nodes = [node for node in self.nodes if 'meta.skippy.io/no-schedule' not in node.labels]
        return self.nodes

    def get_init_image_states(self) -> Dict[str, ImageState]:
        # https://cloud.docker.com/v2/repositories/alexrashed/ml-wf-1-pre/tags/0.36/
        # https://cloud.docker.com/v2/repositories/alexrashed/ml-wf-2-train/tags/0.36/
        # https://cloud.docker.com/v2/repositories/alexrashed/ml-wf-3-serve/tags/0.36/
        return {
            'alexrashed/ml-wf-1-pre:0.37': ImageState(size={
                'arm': 465830200,
                'arm64': 540391110,
                'amd64': 533323136
            }),
            'alexrashed/ml-wf-2-train:0.37': ImageState(size={
                'arm': 519336111,
                'arm64': 594174340,
                'amd64': 550683347
            }),
            'alexrashed/ml-wf-3-serve:0.37': ImageState(size={
                'arm': 511888808,
                'arm64': 590989596,
                'amd64': 589680790
            })
        }

    def get_bandwidth_graph(self) -> Dict[str, Dict[str, float]]:
        # 1.25e+6 Byte/s = 10 MBit/s
        # 1.25e+7 Byte/s = 100 MBit/s
        # 1.25e9 Byte/s = 10 GBit/s - assumed for local access
        # The registry is always connected with 100 MBit/s (replicated in both networks)
        # The edge nodes are interconnected with 100 MBit/s
        # The cloud is connected to the edge nodes with 10 MBit/s
        return {
            'ara-clustercloud1': {
                'ara-clustercloud1': 1.25e+9,
                'ara-clustertegra1': 1.25e+6,
                'ara-clusterpi1': 1.25e+6,
                'ara-clusterpi2': 1.25e+6,
                'ara-clusterpi3': 1.25e+6,
                'ara-clusterpi4': 1.25e+6,
                'ara-clusternuc1': 1.25e+6,
                'ara-clusternuc2': 1.25e+6,
                'registry': 1.25e+7
            },
            'ara-clustertegra1': {
                'ara-clustercloud1': 1.25e+6,
                'ara-clustertegra1': 1.25e+9,
                'ara-clusterpi1': 1.25e+7,
                'ara-clusterpi2': 1.25e+7,
                'ara-clusterpi3': 1.25e+7,
                'ara-clusterpi4': 1.25e+7,
                'ara-clusternuc1': 1.25e+7,
                'ara-clusternuc2': 1.25e+7,
                'registry': 1.25e+7
            },
            'ara-clusterpi1': {
                'ara-clustercloud1': 1.25e+6,
                'ara-clustertegra1': 1.25e+7,
                'ara-clusterpi1': 1.25e+9,
                'ara-clusterpi2': 1.25e+7,
                'ara-clusterpi3': 1.25e+7,
                'ara-clusterpi4': 1.25e+7,
                'ara-clusternuc1': 1.25e+7,
                'ara-clusternuc2': 1.25e+7,
                'registry': 1.25e+7
            },
            'ara-clusterpi2': {
                'ara-clustercloud1': 1.25e+6,
                'ara-clustertegra1': 1.25e+7,
                'ara-clusterpi1': 1.25e+7,
                'ara-clusterpi2': 1.25e+9,
                'ara-clusterpi3': 1.25e+7,
                'ara-clusterpi4': 1.25e+7,
                'ara-clusternuc1': 1.25e+7,
                'ara-clusternuc2': 1.25e+7,
                'registry': 1.25e+7
            },
            'ara-clusterpi3': {
                'ara-clustercloud1': 1.25e+6,
                'ara-clustertegra1': 1.25e+7,
                'ara-clusterpi1': 1.25e+7,
                'ara-clusterpi2': 1.25e+7,
                'ara-clusterpi3': 1.25e+9,
                'ara-clusterpi4': 1.25e+7,
                'ara-clusternuc1': 1.25e+7,
                'ara-clusternuc2': 1.25e+7,
                'registry': 1.25e+7
            },
            'ara-clusterpi4': {
                'ara-clustercloud1': 1.25e+6,
                'ara-clustertegra1': 1.25e+7,
                'ara-clusterpi1': 1.25e+7,
                'ara-clusterpi2': 1.25e+7,
                'ara-clusterpi3': 1.25e+7,
                'ara-clusterpi4': 1.25e+9,
                'ara-clusternuc1': 1.25e+7,
                'ara-clusternuc2': 1.25e+7,
                'registry': 1.25e+7
            },
            'ara-clusternuc1': {
                'ara-clustercloud1': 1.25e+6,
                'ara-clustertegra1': 1.25e+7,
                'ara-clusterpi1': 1.25e+7,
                'ara-clusterpi2': 1.25e+7,
                'ara-clusterpi3': 1.25e+7,
                'ara-clusterpi4': 1.25e+7,
                'ara-clusternuc1': 1.25e+9,
                'ara-clusternuc2': 1.25e+7,
                'registry': 1.25e+7
            },
            'ara-clusternuc2': {
                'ara-clustercloud1': 1.25e+6,
                'ara-clustertegra1': 1.25e+7,
                'ara-clusterpi1': 1.25e+7,
                'ara-clusterpi2': 1.25e+7,
                'ara-clusterpi3': 1.25e+7,
                'ara-clusterpi4': 1.25e+7,
                'ara-clusternuc1': 1.25e+7,
                'ara-clusternuc2': 1.25e+9,
                'registry': 1.25e+7
            }
        }
