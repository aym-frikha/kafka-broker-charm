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
    def __init__(self, data):
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
            self._patch(host, p)

    @patch.object(cluster.KafkaBrokerCluster, "listener_opts",
                  new_callable=PropertyMock)
    @patch.object(cluster.KafkaBrokerCluster, "num_azs",
                  new_callable=PropertyMock)
    @patch.object(cluster.KafkaBrokerCluster, "num_peers",
                  new_callable=PropertyMock)
    @patch.object(zkRelation.ZookeeperRequiresRelation, "get_zookeeper_list")
    @patch.object(kafka.KafkaJavaCharmBase, "create_log_dir")
    @patch.object(charm.KafkaBrokerCharm,
                  'is_client_ssl_enabled')
    @patch.object(charm, "render")
    def test_confluent_simple_render_server_props(self, mock_render,
                                                  mock_is_client_ssl,
                                                  mock_create_log_dir,
                                                  mock_get_zk_list,
                                                  mock_num_peers,
                                                  mock_num_azs,
                                                  mock_listeners):
        mock_listeners.return_value = {}
        mock_num_peers.return_value = 3
        mock_num_azs.return_value = 3
        mock_render.return_value = ""
        mock_is_client_ssl.return_value = False
        crt, key = security.generateSelfSigned("/tmp", "vmdisovs1_testcert")
        mock_get_zk_list.return_value = [
            {"myid": 1, "endpoint": "ansiblezookeeper2.example.com:2888:3888"},
            {"myid": 2, "endpoint": "ansiblezookeeper3.example.com:2888:3888"},
            {"myid": 3, "endpoint": "ansiblezookeeper1.example.com:2888:3888"},
        ]
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
        })
        rel_id = harness.add_relation("zookeeper", "zookeeper") 
        harness.add_relation_unit(rel_id, 'zookeeper/0')        
        rel_id = harness.add_relation("cluster", "kafka-broker")
        harness.add_relation_unit(rel_id, 'kafka-broker/0')     
        zk = harness.charm
        zk._generate_server_properties()
        mock_render.asser_called()
        print(mock_render.call_args_list)
        simulate_render = self._simulate_render(
            ctx=mock_render.call_args.kwargs["context"],
            templ_file='server.properties.j2')
        self.assertEqual(SERVER_PROPS, simulate_render)
