#  Licensed under the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License. You may obtain
#  a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations
#  under the License.

import socket
import builtins
import unittest
import logging
import os
import shutil
from mock import patch, mock_open

import charm as charm

from ops.testing import Harness

import charms.kafka_broker.v0.kafka_base_class as kafka
import charms.kafka_broker.v0.java_class as java
from charms.kafka_broker.v0.kafka_linux import getCurrentUserAndGroup


# Set logger to the module to be mocked
logger = logging.getLogger("charms.kafka_broker.v0.kafka_base_class")

OVERRIDE_CONF = """
[Service]

User=test

Group=test

Environment=\"KAFKA_HEAP_OPTS=-Xmx1g\"
Environment=\"KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/etc/kafka/zookeeper-log4j.properties\"
Environment=\"LOG_DIR=/var/log/kafka\"
"""  # noqa

SVC_ENV_OVERRIDE = """KAFKA_HEAP_OPTS: '-Xmx1g'
KAFKA_LOG4J_OPTS: '-Dlog4j.configuration=file:/etc/kafka/zookeeper-log4j.properties'
LOG_DIR: '/var/log/kafka'""" # noqa


KERBEROS_OVERRIDE_CONF = """
[Service]

User=test

Group=test

Environment=\"KAFKA_HEAP_OPTS=-Xmx1g\"
Environment=\"KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/etc/kafka/zookeeper-log4j.properties\"
Environment=\"LOG_DIR=/var/log/kafka\"
Environment=\"KAFKA_OPTS=-Djdk.tls.ephemeralDHKeySize=2048 -Djava.security.auth.login.config=/etc/kafka/jaas.conf\"
Environment="SCHEMA_REGISTRY_OPTS=-Djdk.tls.ephemeralDHKeySize=2048 -Djava.security.auth.login.config=/etc/kafka/jaas.conf"
Environment="KSQL_OPTS=-Djdk.tls.ephemeralDHKeySize=2048 -Djava.security.auth.login.config=/etc/kafka/jaas.conf"
Environment="KAFKAREST_OPTS=-Djdk.tls.ephemeralDHKeySize=2048 -Djava.security.auth.login.config=/etc/kafka/jaas.conf"
Environment="CONTROL_CENTER_OPTS=-Djdk.tls.ephemeralDHKeySize=2048 -Djava.security.auth.login.config=/etc/kafka/jaas.conf"
"""  # noqa

KERBEROS_SVC_ENV_OVERRIDE = """KAFKA_HEAP_OPTS: '-Xmx1g'
KAFKA_OPTS: "-Djava.security.auth.login.config=/etc/kafka/jaas.conf"
KAFKA_LOG4J_OPTS: '-Dlog4j.configuration=file:/etc/kafka/zookeeper-log4j.properties'
LOG_DIR: '/var/log/kafka'""" # noqa

KERBEROS_JAAS_CONF = """Server {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/security/keytabs/test.keytab"
    storeKey=true
    useTicketCache=false
    principal="HTTP/test.example.com@TEST.COM";
};
""" # noqa

KRB5_CONF = """[libdefaults]
 default_realm = EXAMPLE.COM
 dns_lookup_realm = false
 dns_lookup_kdc = false
 ticket_lifetime = 24h
 forwardable = true
 udp_preference_limit = 1
 default_tkt_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 arc-four-hmac rc4-hmac
 default_tgs_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 arc-four-hmac rc4-hmac
 permitted_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 arc-four-hmac rc4-hmac

[realms]
 EXAMPLE.COM = {
  kdc = ldap.example.com:88
  admin_server = ldap.example.com:749
  default_domain = example.com
 }

[domain_realm]
 .example.com = EXAMPLE.COM
  example.com = EXAMPLE.COM""" # noqa


class TestAppKafka(unittest.TestCase):
    maxDiff = None

    def _simulate_render(self, ctx=None, templ_file=""):
        import jinja2
        env = jinja2.Environment(loader=jinja2.FileSystemLoader('templates'))
        templ = env.get_template(templ_file)
        doc = templ.render(ctx)
        return doc

    def setUp(self):
        super(TestAppKafka, self).setUp()
        os.environ["JUJU_CHARM_DIR"] = "./"

    @patch.object(logger, "warning")
    @patch.object(shutil, "chown")
    @patch.object(os, "makedirs")
    def test_create_log_dir(self,
                            mock_mkdir,
                            mock_chown,
                            mock_warning):
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        k = harness.charm
        # Ensure we can skip if data_log_dev is empty
        k.create_log_dir(data_log_dev=None,
                         data_log_dir="",
                         data_log_fs=None)
        mock_warning.assert_called()

    @patch("os.makedirs")
    @patch.object(kafka, "subprocess")
    @patch.object(kafka, "setFilePermissions")
    @patch.object(kafka.KafkaJavaCharmBase, "set_folders_and_permissions")
    @patch.object(java.JavaCharmBase, "install_packages")
    @patch.object(kafka, "apt_update")
    @patch.object(kafka, "add_source")
    def test_install_packages(self,
                              mock_add_source,
                              mock_apt_update,
                              mock_java_inst_pkgs,
                              mock_set_folders_perms,
                              mock_set_file_perms,
                              mock_subprocess_check,
                              mock_os_makedirs):

        harness = Harness(kafka.KafkaJavaCharmBase)
#        harness = Harness(charm.KafkaBrokerCharm)
        self.addCleanup(harness.cleanup)
        harness.begin()
        #harness._update_config({
        #    "distro": "confluent",
        #    "version": "6.1"
        #})
        k = harness.charm
        k.install_packages("openjdk-11-headless", ["test"])
        self.assertIn(
            "deb [arch=amd64] https://packages.confluent.io/deb/6.1" +
            " stable main", mock_add_source.call_args[0])
        mock_apt_update.assert_called()
        mock_java_inst_pkgs.assert_called()
        # Check subprocess call for jmx exporter
        mock_subprocess_check.check_output.assert_called()
        mock_subprocess_check.check_output.assert_any_call(
            ['wget', '-qO', '/opt/prometheus/jmx_prometheus_javaagent.jar',
             'https://repo1.maven.org/maven2/io/prometheus/jmx/'
             'jmx_prometheus_javaagent/0.12.0/'
             'jmx_prometheus_javaagent-0.12.0.jar'])

    @patch.object(logger, "warning")
    @patch.object(shutil, "chown")
    @patch.object(os, "makedirs")
    def test_create_data_and_log_dirs(self,
                                      mock_mkdir,
                                      mock_chown,
                                      mock_warning):
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        k = harness.charm
        k.create_data_and_log_dirs(
            data_log_dev=None,
            data_dev="",
            data_log_dir="",
            data_dir=None,
            data_log_fs="",
            data_fs=None)
        mock_warning.assert_called()

"""
    @patch("os.makedirs")
    @patch.object(kafka.KafkaJavaCharmBase, "set_folders_and_permissions")
    @patch.object(kafka, "render")
    @patch.object(kafka.KafkaJavaCharmBase, "is_sasl_kerberos_enabled")
    @patch.object(kafka.KafkaJavaCharmBase, "is_ssl_enabled")
    def test_render_override_conf(self,
                                  mock_ssl_enabled,
                                  mock_krbs,
                                  mock_render,
                                  mock_set_folder_perms,
                                  mock_os_makedirs):
        def __cleanup():
            try:
                os.remove("/tmp/13fnutest/13fnutest.service")
                os.remove("/tmp/13fnutest")
            except: # noqa
                pass

        __cleanup()
        mock_render.return_value = ""
        mock_ssl_enabled.return_value = False
        mock_krbs.return_value = False
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        k = harness.charm
        k.service = "13fnutest"
        # We do not have config-changed here
        # and we do not want to trigger it
        user, group = getCurrentUserAndGroup()
        harness._update_config(key_values={
            "user": "test",
            "group": "test",
            "service-unit-overrides": '',
            "service-overrides": "",
            "service-environment-overrides": SVC_ENV_OVERRIDE,
        })
        k.render_service_override_file(
            target="/tmp/13fnutest/13fnutest.service")
        mock_render.assert_called()
        rendered = self._simulate_render(
            ctx=mock_render.call_args.kwargs["context"],
            templ_file="kafka_override.conf.j2")
        self.assertEqual(OVERRIDE_CONF, rendered)
        __cleanup()

    @patch("os.makedirs")
    @patch.object(kafka.KafkaJavaCharmBase, "_render_krb5_conf")
    @patch.object(socket, "gethostname")
    @patch.object(kafka.KafkaJavaCharmBase, "set_folders_and_permissions")
    @patch.object(kafka, "render")
    @patch.object(kafka.KafkaJavaCharmBase, "is_ssl_enabled")
    def test_kerberos_svc_config(self,
                                 mock_ssl_enabled,
                                 mock_render,
                                 mock_set_folder_perms,
                                 mock_gethostname,
                                 mock_render_krb5_conf,
                                 mock_os_makedirs):
        def __cleanup():
            try:
                os.remove("/tmp/rnoetest/rnoetest.service")
                os.remove("/tmp/rnoetest")
            except: # noqa
                pass

        __cleanup()
        mock_gethostname.return_value = "test"
        mock_render.return_value = ""
        mock_ssl_enabled.return_value = True
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        k = harness.charm
        k.service = "rnoetest"
        # We do not have config-changed here
        # and we do not want to trigger it
        user, group = getCurrentUserAndGroup()
        harness._update_config(key_values={
            "user": "test",
            "group": "test",
            "sasl-protocol": "kerberos",
            "kerberos-kdc-hostname": "ldap.example.com",
            "kerberos-admin-hostname": "ldap.example.com",
            "kerberos-protocol": "HTTP",
            "kerberos-domain": "example.com",
            "kerberos-realm": "TEST.COM",
            "service-unit-overrides": '',
            "service-overrides": "",
            "service-environment-overrides": SVC_ENV_OVERRIDE,
        })
        # Testing render service files
        k.render_service_override_file(
            target="/tmp/rnoetest.service/rnoetest.service")
        mock_render.assert_called()
        # Render the file for comparison
        rendered = self._simulate_render(
            ctx=mock_render.call_args.kwargs["context"],
            templ_file="kafka_override.conf.j2")
        self.assertEqual(KERBEROS_OVERRIDE_CONF, rendered)
        __cleanup()


    @patch("os.makedirs")
    @patch.object(kafka.KafkaJavaCharmBase, "_render_krb5_conf")
    @patch.object(socket, "gethostname")
    @patch.object(kafka, "setFilePermissions")
    @patch.object(kafka.KafkaJavaCharmBase, "set_folders_and_permissions")
    def test_kerberos_jaas_config(self,
                                  mock_set_folder_perms,
                                  mock_set_files_perms,
                                  mock_gethostname,
                                  mock_render_krb5_conf,
                                  mock_os_makedirs):
        mock_gethostname.return_value = "test"
        mock_set_files_perms.return_value = None
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        k = harness.charm
        k.keytab = "test.keytab"
        # We do not have config-changed here
        # and we do not want to trigger it
        user, group = getCurrentUserAndGroup()
        harness._update_config(key_values={
            "user": "test",
            "group": "test",
            "sasl-protocol": "kerberos",
            "kerberos-kdc-hostname": "ldap.example.com",
            "kerberos-admin-hostname": "ldap.example.com",
            "kerberos-principal": "test",
            "kerberos-protocol": "HTTP",
            "kerberos-domain": "example.com",
            "kerberos-realm": "TEST.COM",
            "service-unit-overrides": '',
            "service-overrides": "",
            "service-environment-overrides": SVC_ENV_OVERRIDE,
        })
        # Test the config changed routine:
        m_open = mock_open()
        with patch.object(builtins, 'open', m_open):
            k._on_config_changed(None)
            m_open.assert_called_once_with("/etc/kafka/jaas.conf", "w")
            handle = m_open()
            handle.write.assert_called_once_with(KERBEROS_JAAS_CONF)

    @patch("os.makedirs")
    @patch.object(kafka, "render")
    @patch.object(socket, "gethostname")
    @patch.object(kafka, "setFilePermissions")
    @patch.object(kafka.KafkaJavaCharmBase, "set_folders_and_permissions")
    def test_kerberos_krb5_config(self,
                                  mock_set_folder_perms,
                                  mock_set_files_perms,
                                  mock_gethostname,
                                  mock_render,
                                  mock_os_makedirs):
        mock_gethostname.return_value = "test"
        mock_set_files_perms.return_value = None
        harness = Harness(kafka.KafkaJavaCharmBase)
        self.addCleanup(harness.cleanup)
        harness.begin()
        k = harness.charm
        k.keytab = "test.keytab"
        # We do not have config-changed here
        # and we do not want to trigger it
        user, group = getCurrentUserAndGroup()
        harness._update_config(key_values={
            "user": "test",
            "group": "test",
            "sasl-protocol": "kerberos",
            "kerberos-kdc-hostname": "ldap.example.com",
            "kerberos-admin-hostname": "ldap.example.com",
            "kerberos-principal": "example",
            "kerberos-protocol": "HTTP",
            "kerberos-domain": "example.com",
            "kerberos-realm": "EXAMPLE.COM",
            "service-unit-overrides": '',
            "service-overrides": "",
            "service-environment-overrides": SVC_ENV_OVERRIDE,
        })
        k._render_krb5_conf()
        mock_render.assert_called()
        # Render the file for comparison
        rendered = self._simulate_render(
            ctx=mock_render.call_args.kwargs["context"],
            templ_file="krb5.conf.j2")
        self.assertEqual(KRB5_CONF, rendered)
"""
