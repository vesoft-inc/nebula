# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import json
import logging
import yaml
import string
import random
import time

from kubernetes import client, config

NEBULA_OPERATOR_GROUP = "apps.nebula-graph.io"
NEBULA_OPERATOR_VERSION = "v1alpha1"
NEBULA_OPERATOR_PLURAL = "nebulaclusters"

NEBULA_OPERATOR_TEMPLATE = """
apiVersion: apps.nebula-graph.io/v1alpha1
kind: NebulaCluster
metadata:
  name: {nebulacluster_name}
spec:
  graphd:
    config:
      heartbeat_interval_secs: "1"
    resources:
      requests:
        cpu: "500m"
        memory: "500Mi"
      limits:
        cpu: "1"
        memory: "2Gi"
    replicas: {graphd_num}
    image: {docker_group}/nebula-graphd
    version: "{docker_tag}"
    service:
      type: NodePort
      externalTrafficPolicy: Local
    storageClaim:
      resources:
        requests:
          storage: 2Gi
      storageClassName: nfs-client
  metad:
    config:
      heartbeat_interval_secs: "1"
    resources:
      requests:
        cpu: "500m"
        memory: "500Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
    replicas: {metad_num}
    image: {docker_group}/nebula-metad
    version: "{docker_tag}"
    storageClaim:
      resources:
        requests:
          storage: 2Gi
      storageClassName: nfs-client
  storaged:
    config:
      heartbeat_interval_secs: "1"
    resources:
      requests:
        cpu: "500m"
        memory: "500Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
    replicas: {storaged_num}
    image: {docker_group}/nebula-storaged
    version: "{docker_tag}"
    storageClaim:
      resources:
        requests:
          storage: 1Gi
      storageClassName: nfs-client
  reference:
    name: statefulsets.apps
    version: v1
  schedulerName: default-scheduler
  imagePullPolicy: Always
  enablePVReclaim: true
"""


def name_generator(size=6, chars=string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))


class K8SUtil(object):
    def __init__(self, kubeconfig, namespace="default") -> None:
        super().__init__()
        self.kubeconfig = kubeconfig
        self.namespace = namespace
        config.load_kube_config(config_file=self.kubeconfig)

        self.v1 = client.CoreV1Api()
        self.custom_api = client.CustomObjectsApi()

    def get_one_node_ip(self):
        res = self.v1.list_node()
        for i in res.items:
            for addr in i.status.addresses:
                if addr.type == "InternalIP":
                    return addr.address

    def create_nebulacluster(self, graphd_num, metad_num, storaged_num, docker_group, docker_tag):
        name = name_generator()
        body_str = NEBULA_OPERATOR_TEMPLATE.format(
            nebulacluster_name=name,
            graphd_num=graphd_num,
            storaged_num=storaged_num,
            metad_num=metad_num,
            docker_group=docker_group,
            docker_tag=docker_tag,
        )
        body = yaml.load(body_str, Loader=yaml.FullLoader)
        res = self.custom_api.create_namespaced_custom_object(
            NEBULA_OPERATOR_GROUP,
            NEBULA_OPERATOR_VERSION,
            self.namespace,
            NEBULA_OPERATOR_PLURAL,
            body,
        )

        if not self.wait_nebulacluster_ready(name):
            raise Exception("create nebula cluster failed")

        # patch nebula svc, using nodeport
        self.patch_nebulacluster_svc(name)

        # TODO wait for svc more graceful.
        time.sleep(1)

        return res, name

    def get_nebulacluster(self, name):
        res = self.custom_api.get_namespaced_custom_object(
            NEBULA_OPERATOR_GROUP,
            NEBULA_OPERATOR_VERSION,
            self.namespace,
            NEBULA_OPERATOR_PLURAL,
            name,
        )
        return res

    def wait_nebulacluster_ready(self, name, timeout=180):
        """wait for nebulacluster is ready

        Args:
            name ([type]): nebula name
            timeout (int, optional): Defaults to 60 second.
        """
        for i in range(timeout):
            nebula = self.get_nebulacluster(name)
            # wait for status
            if nebula.get("status") is None:
                time.sleep(1)
                continue
            try:
                if nebula["status"]["conditions"][0]["status"] == "True":
                    return True
            except Exception as e:
                logging.error("cannot get the nebulacluster status, error is {}".format(e))
                return False
            time.sleep(1)
        logging.error("timeout for waiting the nebulacluster ready")
        return False

    def delete_nebulacluster(self, name):
        res = self.custom_api.delete_namespaced_custom_object(
            NEBULA_OPERATOR_GROUP,
            NEBULA_OPERATOR_VERSION,
            self.namespace,
            NEBULA_OPERATOR_PLURAL,
            name,
        )

        return res

    def patch_nebulacluster_svc(self, name):
        svc_name = "{}-graphd-svc".format(name)
        body = client.V1Service(
            spec=client.V1ServiceSpec(type="NodePort"),
        )
        res = self.v1.patch_namespaced_service(svc_name, self.namespace, body)
        # TODO verify patch successfully
        return True

    def get_service(self, svc_name):
        return self.v1.list_namespaced_service(
            namespace=self.namespace, field_selector="metadata.name={}".format(svc_name)
        )

    def get_graph_svc_nodeport(self, name):
        services = self.get_service("{}-graphd-svc".format(name))
        assert isinstance(services, client.V1ServiceList)
        if len(services.items) != 1:
            return

        graph_svc = services.items[0]
        assert isinstance(graph_svc, client.V1Service)

        for p in graph_svc.spec.ports:
            assert isinstance(p, client.V1ServicePort)
            if p.name == "thrift":
                return p.node_port
