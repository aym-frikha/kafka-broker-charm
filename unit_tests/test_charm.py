# Copyright 2021 pguimaraes
# See LICENSE file for licensing details.

import os
import unittest
from mock import patch
from mock import PropertyMock

from ops.testing import Harness
import src.charm as charm
import src.cluster as cluster
import charmhelpers.core.host as host
import charmhelpers.fetch.ubuntu as ubuntu

from unit_tests.config_files import SERVER_PROPS

from wand.contrib.linux import getCurrentUserAndGroup
import wand.apps.relations.zookeeper as zkRelation
import wand.apps.kafka as kafka
import wand.contrib.java as java
import wand.security as security

TO_PATCH_FETCH = [
    'apt_install',
    'apt_update',
    'add_source'
]

TO_PATCH_HOST = [
    'service_running',
    'service_restart',
    'service_reload'
]


class MockRelation(object):
    def __init__(self, data=None):
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def units(self):
        return list(self._data.keys())


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
        for p in TO_PATCH_FETCH:
            self._patch(ubuntu, p)
        for p in TO_PATCH_HOST:
            self._patch(charm, p)

    # For _on_install
    @patch.object(charm.KafkaBrokerCharm, "create_log_dir")
    @patch.object(kafka.KafkaJavaCharmBase, "install_packages")
    # For _on_config_changed
    @patch.object(charm.KafkaBrokerCharm, "render_service_override_file")
    @patch.object(cluster.KafkaBrokerCluster, "unit",
                  new_callable=PropertyMock)
    @patch.object(cluster.KafkaBrokerCluster, "relation",
                  new_callable=PropertyMock)
    @patch.object(zkRelation.ZookeeperRequiresRelation, "unit",
                  new_callable=PropertyMock)
    @patch.object(zkRelation.ZookeeperRequiresRelation, "relation",
                  new_callable=PropertyMock)
#    @patch.object(cluster.KafkaBrokerCluster, "listener_opts",
#                  new_callable=PropertyMock)
    @patch.object(cluster.KafkaBrokerCluster, "num_azs",
                  new_callable=PropertyMock)
    @patch.object(cluster.KafkaBrokerCluster, "num_peers",
                  new_callable=PropertyMock)
    @patch.object(charm.KafkaBrokerCharm,
                  'is_client_ssl_enabled')
    @patch.object(charm, "render")
    def test_confluent_config_changed_call(self, mock_render,
                                           mock_is_client_ssl,
                                           mock_num_peers,
                                           mock_num_azs,
#                                           mock_listeners,
                                           mock_zk_rel_data,
                                           mock_zk_unit,
                                           mock_cluster_data,
                                           mock_cluster_unit,
                                           mock_render_svc,
                                           mock_inst_packages,
                                           mock_create_log_dir):
        def __cleanup():
            for i in ["/tmp/vmdisovs1_testcert.crt",
                      "/tmp/vmdisovs1_testcert.key",
                      "/tmp/15fsnuw_ks.jks",
                      "/tmp/15fsnuw_ts.jks",
                      "/tmp/15fsnuw_zk_ks.jks",
                      "/tmp/15fsnuw_zk_ts.jks"]:
                try:
                    os.remove(i)
                except:  # noqa
                    pass

        __cleanup()
#        mock_listeners.return_value = {}
        mock_num_peers.return_value = 3
        mock_num_azs.return_value = 3
        mock_render.return_value = ""
        mock_is_client_ssl.return_value = False
        crt, key = security.generateSelfSigned("/tmp", "vmdisovs1_testcert")
        user, group = getCurrentUserAndGroup()
        harness = Harness(charm.KafkaBrokerCharm)
        self.addCleanup(harness.cleanup)
        harness.begin()
        os.environ["JUJU_AVAILABILITY_ZONE"] = "test"
        # _update_config: do not run a config-changed hook
        harness._update_config({
            "user": user,
            "group": group,
            "log.dirs": "ext4: /tmp/",
            "customize-failure-domain": True,
            "replication-factor": 3,
            "generate-root-ca": True,
            "internal-cluster-domain": "maas",
            "client-cluster-domain": "maas",
            "broker-cluster-domain": "maas",
            "keystore-path": "/tmp/15fsnuw_ks.jks",
            "truststore-path": "/tmp/15fsnuw_ts.jks",
            "keystore-zookeeper-path": "/tmp/15fsnuw_zk_ks.jks",
            "truststore-zookeeper-path": "/tmp/15fsnuw_zk_ts.jks",
            "data-log-dir": {"ext4": "/var/lib/kafka"}
        })
        mock_zk_rel_data.return_value = MockRelation(data = {
            "this": {},
            "zookeeper": {"mtls_cert": crt, "endpoint": "zookeeper.maas:2182"}
        })
        mock_zk_unit.return_value = "this"
        mock_cluster_data.return_value = MockRelation(data = {
            "this": {},
            "otherunit1": {"az": "2", "tls_cert": crt}
        })
        mock_cluster_unit.return_value = "this"
        kafka = harness.charm
        kafka._on_install(None)
        kafka.cluster.on_cluster_relation_joined(None)
        kafka._on_zookeeper_relation_changed(None)
        kafka._on_config_changed(None)
        __cleanup()
        mock_render.assert_called()
        # There are 5x calls to render: (1) tls-client-properties,
        # (2) server.props (for _on_install)
        # (3) client.props and then (4) tls-client.props (config-changed)
        # (5) server.props (on_config_changed)
        # (6) client.props rendering
        # Interested on the output of the 5th call
        server_props = mock_render.call_args_list[4].kwargs["context"]
        print(server_props)
        # clean up values that are randomly generated or depend on the machine:
        server_props["server_props"]["listeners"] = "internal://vm.maas:9092,broker://vm.maas:9093,client://vm.maas:9094"
        server_props["server_props"]["advertised.listeners"] = "internal://vm.maas:9092,broker://vm.maas:9093,client://vm.maas:9094"
        server_props["server_props"]["listener.name.client.ssl.truststore.password"] = "confluenttruststorepass"
        server_props["server_props"]["listener.name.client.ssl.keystore.password"] = "confluentkeystorepass"
        server_props["server_props"]["listener.name.internal.ssl.truststore.password"] = "confluenttruststorepass"
        server_props["server_props"]["listener.name.internal.ssl.keystore.password"] = "confluentkeystorepass"
        server_props["server_props"]["listener.name.broker.ssl.truststore.password"] = "confluenttruststorepass"
        server_props["server_props"]["listener.name.broker.ssl.keystore.password"] = "confluentkeystorepass"
        server_props["server_props"]["zookeeper.ssl.truststore.password"] = "confluenttruststorepass"
        simulate_render = self._simulate_render(
            ctx=server_props,
            templ_file='server.properties.j2')
        print(simulate_render)
        self.assertEqual(SERVER_PROPS, simulate_render)
