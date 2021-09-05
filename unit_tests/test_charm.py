# Copyright 2021 pguimaraes
# See LICENSE file for licensing details.

import os
import unittest
import shutil
from mock import patch
from mock import PropertyMock
import base64

from ops.testing import Harness
import charm as charm
import cluster as cluster
import charmhelpers.fetch.ubuntu as ubuntu

import wand.contrib.java as java
from nrpe.client import NRPEClient

import wand.apps.relations.kafka_relation_base as kafka_relation_base

from unit_tests.config_files import SERVER_PROPS, SERVER_PROPS_LISTENERS

import wand.apps.kafka as kafka
import wand.security as security

from wand.apps.relations.tls_certificates import (
    TLSCertificateRequiresRelation,
)

import wand.apps.relations.kafka_listener as kafka_listener

TO_PATCH_LINUX = [
    "userAdd",
    "groupAdd"
]

TO_PATCH_FETCH = [
    'apt_install',
    'apt_update',
    'add_source'
]

TO_PATCH_HOST = [
    'service_resume',
    'service_running',
    'service_restart'
]


class MockEvent(object):
    def __init__(self, relations):
        self._relations = relations

    @property
    def relation(self):
        return self._relations


class MockRelations(object):
    def __init__(self, data=None):
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def relation(self):
        return self._data

    @property
    def relations(self):
        return [self]

    @property
    def units(self):
        return list(self._data.keys())


class MockOpsCoordinator(object):
    def __init__(self):
        super().__init__()

    def resume(self):
        return

    def release(self):
        return


class TestCharm(unittest.TestCase):
    maxDiff = None  # print the entire diff on assert commands

    def _patch(self, obj, method):
        _m = patch.object(obj, method)
        mock = _m.start()
        self.addCleanup(_m.stop)
        return mock

    def _simulate_render(self, ctx=None, templ_file=""):
        import jinja2
        env = jinja2.Environment(loader=jinja2.FileSystemLoader('templates'))
        templ = env.get_template(templ_file)
        doc = templ.render(ctx)
        return doc

    def setUp(self):
        super(TestCharm, self).setUp()
        for p in TO_PATCH_LINUX:
            self._patch(kafka, p)
        for p in TO_PATCH_FETCH:
            self._patch(ubuntu, p)
        for p in TO_PATCH_HOST:
            self._patch(charm, p)

    @patch.object(shutil, "which")
    @patch.object(os, "makedirs")
    @patch.object(charm, "OpsCoordinator")
    @patch.object(kafka_listener.KafkaListenerProvidesRelation,
                  'advertise_addr', new_callabl=PropertyMock)
    @patch.object(cluster.KafkaBrokerCluster,
                  'advertise_addr', new_callable=PropertyMock)
    @patch.object(kafka_listener.KafkaListenerProvidesRelation,
                  'binding_addr', new_callabl=PropertyMock)
    @patch.object(cluster.KafkaBrokerCluster,
                  'binding_addr', new_callable=PropertyMock)
    @patch.object(kafka_listener, 'get_hostname')
    @patch.object(cluster, 'get_hostname')
    @patch.object(kafka_relation_base, 'CreateTruststore')
    @patch.object(charm, 'CreateTruststore')
    @patch.object(charm, 'open_port')
    @patch.object(charm.KafkaBrokerCharm, '_check_if_ready_to_start')
    @patch.object(charm, 'service_resume')
    @patch.object(charm, 'service_restart')
    @patch.object(charm, 'service_running')
    @patch.object(charm.KafkaBrokerCharm, '_generate_keystores')
    @patch.object(charm.KafkaBrokerCharm, '_cert_relation_set')
    @patch.object(kafka, "open_port")
    @patch.object(NRPEClient, "add_check")
    @patch.object(kafka.KafkaJavaCharmBasePrometheusMonitorNode,
                  'advertise_addr', new_callable=PropertyMock)
    @patch.object(kafka.KafkaJavaCharmBasePrometheusMonitorNode,
                  'scrape_request', new_callable=PropertyMock)
    @patch.object(java, "genRandomPassword")
    @patch.object(charm, "genRandomPassword")
    # Those two patches set _cert_relation_set + get_ssl* as empty
    @patch.object(TLSCertificateRequiresRelation, "get_server_certs")
    @patch.object(TLSCertificateRequiresRelation, "request_server_cert")
    @patch.object(charm, "PKCS12CreateKeystore")
    @patch.object(charm.KafkaBrokerCharm, "create_data_and_log_dirs")
    @patch.object(kafka.KafkaJavaCharmBase, "install_packages")
    @patch.object(charm.KafkaBrokerCharm, "set_folders_and_permissions")
    @patch.object(kafka.KafkaJavaCharmBase, "create_log_dir")
    @patch.object(kafka, "render")
    @patch.object(charm, "render")
    def test_config_changed(self,
                            mock_render,
                            mock_kafka_class_render,
                            mock_create_log_dir,
                            mock_folders_perms,
                            mock_on_install_pkgs,
                            mock_create_dirs,
                            mock_create_jks,
                            mock_request_server_cert,
                            mock_get_server_certs,
                            mock_gen_random_pwd,
                            mock_java_gen_random_pwd,
                            mock_prometheus_scrape_req,
                            mock_prometheus_advertise_addr,
                            mock_nrpe_add_check,
                            mock_kafka_open_port,
                            mock_cert_relation_set,
                            mock_generate_keystores,
                            mock_service_running,
                            mock_svc_restart,
                            mock_svc_resume,
                            mock_check_if_ready_restart,
                            mock_open_port,
                            mock_create_ts,
                            mock_rel_base_create_ts,
                            mock_get_hostname,
                            mock_list_get_hostname,
                            mock_cluster_binding_addr,
                            mock_list_binding_addr,
                            mock_cluster_advertise_addr,
                            mock_list_advertise_addr,
                            mock_ops_coordinator,
                            mock_os_makedirs,
                            mock_shutil_which):
        """Test configuration changed with a cluster + 1x unit ZK.
        Use certificates passed via options and this is leader unit.
        Check each of the properties generated using mock_render.
        """
        mock_shutil_which.return_value = True
        # Avoid triggering the RestartEvent
        mock_service_running.return_value = False
        mock_check_if_ready_restart.return_value = False
        mock_get_hostname.return_value = \
            "vm.maas"
        mock_list_get_hostname.return_value = \
            "vm.maas"
        # Ensure addresses are set
        mock_cluster_binding_addr.return_value = "192.168.200.200"
        mock_list_binding_addr.return_value = "192.168.200.200"
        mock_cluster_advertise_addr.return_value = "192.168.200.200"
        mock_list_advertise_addr.return_value = "192.168.200.200"
        # Remove the random password generation
        mock_gen_random_pwd.return_value = "confluentkeystorepass"
        mock_java_gen_random_pwd.return_value = "confluentkeystorepass"
        # Mock the OpsCoordinator
        mock_ops_coordinator.return_value = MockOpsCoordinator()
        # Prepare cleanup

        def __cleanup():
            for i in ["/tmp/testcert*", "/tmp/test-ts-quorum.jks"]:
                try:
                    os.remove(i)
                except: # noqa
                    pass

        __cleanup()
        certs = {}
        crt, key = security.generateSelfSigned("/tmp", "testcert")
        for i in range(1, 4):
            certs[i] = {}
            certs[i]["crt"] = crt
            certs[i]["key"] = key
        __cleanup()  # cleaning up the intermediate certs

        # Mock-up values
        mock_cert_relation_set.return_value = True
        # Prepare test
        harness = Harness(charm.KafkaBrokerCharm)
        harness.update_config({
            "version": "6.1",
            "distro": "confluent",
            "user": "test",
            "group": "test",
            "keystore-zookeeper-path": "/var/ssl/private/zk-ks.jks",
            "truststore-zookeeper-path": "/var/ssl/private/zk-ts.jks",
            "truststore-path": "/var/ssl/private/ssl-ts.jks",
            "keystore-path": "/var/ssl/private/ssl-ks.jks",
            "ssl_cert": base64.b64encode(crt.encode("ascii")),
            "ssl_key": base64.b64encode(key.encode("ascii")),
            "ssl-zk-cert": base64.b64encode(crt.encode("ascii")),
            "ssl-zk-key": base64.b64encode(key.encode("ascii")),
            "replication-factor": 3,
            "customize-failure-domain": False,
            "generate-root-ca": False,
            "service-restart-on-fail": True,
            "generate-root-ca": False,
            "log4j-root-logger": "DEBUG, stdout, kafkaAppender"
        })
        harness.set_leader(True)
        # Complete the cluster
        cluster_id = harness.add_relation("cluster", "kafka-broker")
        harness.add_relation_unit(cluster_id, "kafka-broker/1")
        harness.update_relation_data(cluster_id, "kafka-broker/1", {
            "cert": certs[1]["crt"],
            "prometheus-manual_endpoint": "192.168.100.101"
        })
        harness.add_relation_unit(cluster_id, "kafka-broker/2")
        harness.update_relation_data(cluster_id, "kafka-broker/2", {
            "cert": certs[2]["crt"],
            "prometheus-manual_endpoint": "192.168.100.102"
        })
        # Zookeeper relation
        zk_id = harness.add_relation("zookeeper", "zookeeper")
        harness.add_relation_unit(zk_id, "zookeeper/0")
        harness.update_relation_data(zk_id, "zookeeper/0", {
            "cert": certs[3]["crt"],
            "endpoint": "zookeeper.maas:2182"
        })
        harness.begin_with_initial_hooks()
        self.addCleanup(harness.cleanup)
        kafka = harness.charm
        # If cluster relation events (-joined, -changed) happens before
        # certificate events, then cluster-* will be deferred.
        # Run reemit to ensure they are run.
        kafka.framework.reemit()
        args, kwargs = mock_render.call_args_list[-2]
        # Check server.properties rendering
        server_properties = SERVER_PROPS.split("\n")
        server_properties.sort()
        render_server_props = self._simulate_render(
            kwargs["context"], templ_file="server.properties.j2").split("\n")
        render_server_props.sort()
        self.assertEqual(server_properties, render_server_props)
        # Assert client.properties was correctly rendered
        mock_render.assert_any_call(
            source='client.properties.j2',
            target='/etc/kafka/client.properties',
            owner='test', group='test', perms=0o640,
            context={
                'client_props': {
                    'default.api.timeout.ms': 20000,
                    'request.timeout.ms': 20000,
                    'ssl.truststore.location': '/var/ssl/private/ssl-ts.jks',
                    'ssl.truststore.password': 'confluentkeystorepass'}}
        )
        # keystore is set, assert generate_keystores was called
        mock_generate_keystores.assert_called()
        # Zookeeper TLS properties
        mock_render.assert_any_call(
            source='zookeeper-tls-client.properties.j2',
            target='/etc/kafka/zookeeper-tls-client.properties',
            owner='test', group='test', perms=0o640,
            context={'client_props': {
                'zookeeper.clientCnxnSocket': 'org.apache.zookeeper.'
                                              'ClientCnxnSocketNetty',
                'zookeeper.ssl.client.enable': 'true',
                'zookeeper.ssl.keystore.location':
                '/var/ssl/private/zk-ks.jks',
                'zookeeper.ssl.keystore.password': 'confluentkeystorepass',
                'zookeeper.ssl.truststore.location':
                '/var/ssl/private/zk-ts.jks',
                'zookeeper.ssl.truststore.password': 'confluentkeystorepass'}}
        )

    @patch.object(shutil, "which")
    @patch.object(os, "makedirs")
    @patch.object(charm, "OpsCoordinator")
    @patch.object(kafka_listener.KafkaListenerProvidesRelation,
                  'advertise_addr', new_callabl=PropertyMock)
    @patch.object(cluster.KafkaBrokerCluster,
                  'advertise_addr', new_callable=PropertyMock)
    @patch.object(kafka_listener.KafkaListenerProvidesRelation,
                  'binding_addr', new_callabl=PropertyMock)
    @patch.object(cluster.KafkaBrokerCluster,
                  'binding_addr', new_callable=PropertyMock)
    @patch.object(kafka_listener, 'get_hostname')
    @patch.object(cluster, 'get_hostname')
    @patch.object(kafka_relation_base, 'CreateTruststore')
    @patch.object(charm, 'CreateTruststore')
    @patch.object(charm, 'open_port')
    @patch.object(charm.KafkaBrokerCharm, '_check_if_ready_to_start')
    @patch.object(charm, 'service_resume')
    @patch.object(charm, 'service_restart')
    @patch.object(charm, 'service_running')
    @patch.object(charm.KafkaBrokerCharm, '_generate_keystores')
    @patch.object(charm.KafkaBrokerCharm, '_cert_relation_set')
    @patch.object(kafka, "open_port")
    @patch.object(NRPEClient, "add_check")
    @patch.object(kafka.KafkaJavaCharmBasePrometheusMonitorNode,
                  'advertise_addr', new_callable=PropertyMock)
    @patch.object(kafka.KafkaJavaCharmBasePrometheusMonitorNode,
                  'scrape_request', new_callable=PropertyMock)
    @patch.object(java, "genRandomPassword")
    @patch.object(charm, "genRandomPassword")
    # Those two patches set _cert_relation_set + get_ssl* as empty
    @patch.object(TLSCertificateRequiresRelation, "get_server_certs")
    @patch.object(TLSCertificateRequiresRelation, "request_server_cert")
    @patch.object(charm, "PKCS12CreateKeystore")
    @patch.object(charm.KafkaBrokerCharm, "create_data_and_log_dirs")
    @patch.object(kafka.KafkaJavaCharmBase, "install_packages")
    @patch.object(charm.KafkaBrokerCharm, "set_folders_and_permissions")
    @patch.object(kafka.KafkaJavaCharmBase, "create_log_dir")
    @patch.object(kafka, "render")
    @patch.object(charm, "render")
    def test_config_listene(self,
                            mock_render,
                            mock_kafka_class_render,
                            mock_create_log_dir,
                            mock_folders_perms,
                            mock_on_install_pkgs,
                            mock_create_dirs,
                            mock_create_jks,
                            mock_request_server_cert,
                            mock_get_server_certs,
                            mock_gen_random_pwd,
                            mock_java_gen_random_pwd,
                            mock_prometheus_scrape_req,
                            mock_prometheus_advertise_addr,
                            mock_nrpe_add_check,
                            mock_kafka_open_port,
                            mock_cert_relation_set,
                            mock_generate_keystores,
                            mock_service_running,
                            mock_svc_restart,
                            mock_svc_resume,
                            mock_check_if_ready_restart,
                            mock_open_port,
                            mock_create_ts,
                            mock_rel_base_create_ts,
                            mock_get_hostname,
                            mock_list_get_hostname,
                            mock_cluster_binding_addr,
                            mock_list_binding_addr,
                            mock_cluster_advertise_addr,
                            mock_list_advertise_addr,
                            mock_ops_coordinator,
                            mock_os_makedirs,
                            mock_shutil_which):
        """Test configuration changed with a cluster + 1x unit ZK.
        Use certificates passed via options and this is leader unit.
        Add listener relations with 2x applications but no SASL.
        """
        mock_shutil_which.return_value = True
        # Avoid triggering the RestartEvent
        mock_service_running.return_value = False
        mock_check_if_ready_restart.return_value = False
        mock_get_hostname.return_value = \
            "vm.maas"
        mock_list_get_hostname.return_value = \
            "vm.maas"
        # Ensure addresses are set
        mock_cluster_binding_addr.return_value = "192.168.200.200"
        mock_list_binding_addr.return_value = "192.168.200.200"
        mock_cluster_advertise_addr.return_value = "192.168.200.200"
        mock_list_advertise_addr.return_value = "192.168.200.200"
        # Remove the random password generation
        mock_gen_random_pwd.return_value = "confluentkeystorepass"
        mock_java_gen_random_pwd.return_value = "confluentkeystorepass"
        # Mock the OpsCoordinator
        mock_ops_coordinator.return_value = MockOpsCoordinator()
        # Prepare cleanup

        def __cleanup():
            for i in ["/tmp/testcert*", "/tmp/test-ts-quorum.jks"]:
                try:
                    os.remove(i)
                except: # noqa
                    pass

        __cleanup()
        certs = {}
        crt, key = security.generateSelfSigned("/tmp", "testcert")
        for i in range(1, 6):
            certs[i] = {}
            certs[i]["crt"] = crt
            certs[i]["key"] = key
        __cleanup()  # cleaning up the intermediate certs

        # Mock-up values
        mock_cert_relation_set.return_value = True
        # Prepare test
        harness = Harness(charm.KafkaBrokerCharm)
        harness.update_config({
            "version": "6.1",
            "distro": "confluent",
            "user": "test",
            "group": "test",
            "keystore-zookeeper-path": "/var/ssl/private/zk-ks.jks",
            "truststore-zookeeper-path": "/var/ssl/private/zk-ts.jks",
            "truststore-path": "/var/ssl/private/ssl-ts.jks",
            "keystore-path": "/var/ssl/private/ssl-ks.jks",
            "ssl_cert": base64.b64encode(crt.encode("ascii")),
            "ssl_key": base64.b64encode(key.encode("ascii")),
            "ssl-zk-cert": base64.b64encode(crt.encode("ascii")),
            "ssl-zk-key": base64.b64encode(key.encode("ascii")),
            "replication-factor": 3,
            "customize-failure-domain": False,
            "generate-root-ca": False,
            "service-restart-on-fail": True,
            "generate-root-ca": False,
            "log4j-root-logger": "DEBUG, stdout, kafkaAppender"
        })
        harness.set_leader(True)
        # Complete the cluster
        cluster_id = harness.add_relation("cluster", "kafka-broker")
        harness.add_relation_unit(cluster_id, "kafka-broker/1")
        harness.update_relation_data(cluster_id, "kafka-broker/1", {
            "cert": certs[1]["crt"]
        })
        harness.add_relation_unit(cluster_id, "kafka-broker/2")
        harness.update_relation_data(cluster_id, "kafka-broker/2", {
            "cert": certs[2]["crt"]
        })
        # Zookeeper relation
        zk_id = harness.add_relation("zookeeper", "zookeeper")
        harness.add_relation_unit(zk_id, "zookeeper/0")
        harness.update_relation_data(zk_id, "zookeeper/0", {
            "cert": certs[3]["crt"],
            "endpoint": "zookeeper.maas:2182"
        })
        # Add listener relations
        list_1_id = harness.add_relation("listeners", "schema-registry")
        harness.add_relation_unit(list_1_id, "schema-registry/0")
        # Set app data
        harness.update_relation_data(list_1_id, "schema-registry", {
            "request": """{{
                "is_public": "True",
                "plaintext_pwd": "",
                "secprot": "SSL",
                "cert": "{}"
            }}""".format(base64.b64encode(certs[4]["crt"].encode("ascii"))),
            "cert": certs[4]["crt"]
        })
        list_2_id = harness.add_relation("listeners", "kafka-connect")
        harness.add_relation_unit(list_2_id, "kafka-connect/0")
        # Set app data
        harness.update_relation_data(list_2_id, "kafka-connect", {
            "request": """{{
                "is_public": "True",
                "plaintext_pwd": "",
                "secprot": "SSL",
                "cert": "{}"
            }}""".format(base64.b64encode(certs[5]["crt"].encode("ascii"))),
            "cert": certs[5]["crt"]
        })
        harness.begin_with_initial_hooks()
        self.addCleanup(harness.cleanup)
        kafka = harness.charm
        # If cluster relation events (-joined, -changed) happens before
        # certificate events, then cluster-* will be deferred.
        # Run reemit to ensure they are run.
        kafka.framework.reemit()
        args, kwargs = mock_render.call_args_list[-2]
        # Check server.properties rendering
        server_properties = SERVER_PROPS_LISTENERS.split("\n")
        server_properties.sort()
        render_server_props = self._simulate_render(
            kwargs["context"], templ_file="server.properties.j2").split("\n")
        render_server_props.sort()
        self.assertEqual(server_properties, render_server_props)
        # Assert client.properties was correctly rendered
        mock_render.assert_any_call(
            source='client.properties.j2',
            target='/etc/kafka/client.properties',
            owner='test', group='test', perms=0o640,
            context={
                'client_props': {
                    'default.api.timeout.ms': 20000,
                    'request.timeout.ms': 20000,
                    'ssl.truststore.location': '/var/ssl/private/ssl-ts.jks',
                    'ssl.truststore.password': 'confluentkeystorepass'}}
        )
        # keystore is set, assert generate_keystores was called
        mock_generate_keystores.assert_called()
        # Zookeeper TLS properties
        mock_render.assert_any_call(
            source='zookeeper-tls-client.properties.j2',
            target='/etc/kafka/zookeeper-tls-client.properties',
            owner='test', group='test', perms=0o640,
            context={'client_props': {
                'zookeeper.clientCnxnSocket': 'org.apache.zookeeper.'
                                              'ClientCnxnSocketNetty',
                'zookeeper.ssl.client.enable': 'true',
                'zookeeper.ssl.keystore.location':
                '/var/ssl/private/zk-ks.jks',
                'zookeeper.ssl.keystore.password': 'confluentkeystorepass',
                'zookeeper.ssl.truststore.location':
                '/var/ssl/private/zk-ts.jks',
                'zookeeper.ssl.truststore.password': 'confluentkeystorepass'}}
        )
