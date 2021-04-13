#!/usr/bin/env python3
# Copyright 2021 pguimaraes
# See LICENSE file for licensing details.

import subprocess
import logging
import yaml

from ops.charm import CharmBase
from ops.main import main
from ops.framework import StoredState
from ops.model import MaintenanceStatus, ActiveStatus

from charmhelpers.fetch import (
    apt_update,
    add_source,
    apt_install
)
from charmhelpers.core import render
from charmhelpers.core.hookenv import (
    is_leader
)

from wand.contrib.java import KafkaJavaCharmBase
from cluster import KafkaBrokerCluster
from wand.apps.relations.zookeeper import ZookeeperRequiresRelation

logger = logging.getLogger(__name__)

# Given: https://docs.confluent.io/current/installation/cp-ansible/ansible-configure.html
# Setting confluent-server by default
CONFLUENT_PACKAGES = [
  "confluent-common",
  "confluent-rest-utils",
  "confluent-metadata-service",
  "confluent-ce-kafka-http-server",
  "confluent-kafka-rest",
  "confluent-server-rest",
  "confluent-telemetry",
  "confluent-server",
  "confluent-rebalancer",
  "confluent-security",
]

KAFKA_BROKER_SERVICE_TARGET="/lib/systemd/system/{}.service"

class KafkaBrokerCharm(KafkaJavaCharmBase):

    def _install_tarball(self):
        # Use _generate_service_files here
        raise Exception("Not Implemented Yet")

    # From: https://github.com/confluentinc/cp-ansible/blob/b711fc9e3b43d2069a9ac8b13177e7f2a07c7bfb/roles/confluent.kafka_broker/defaults/main.yml#L10
    def _collect_java_args(self):
        java_args = ["-Djdk.tls.ephemeralDHKeySize=2048"] # We always use broker listeners
        ## TODO: if JMX Exporter relation, then we should have: -javaagent:{{jmxexporter_jar_path}}={{kafka_broker_jmxexporter_port}}:{{kafka_broker_jmxexporter_config_path}}
        ## TODO: If GSSAPI or SASL enabled, check the extra parameters
        java_args = java_args + self.config.get("java_extra_args","").split()
        return java_args

    # Used for the archive install_method
    def _generate_service_files(self):
        kafka_broker_service_unit_overrides = yaml.safe_load(
            self.config.get('service-unit-overrides',""))
        kafka_broker_service_overrides = yaml.safe_load(
            self.config.get('service-overrides',""))
        kafka_broker_service_environment_overrides = yaml.safe_load(
            self.config.get('service-environment-overrides',""))
        target = None
        if self.distro == "confluent":
            target = "/lib/systemd/system/confluent-server.service"
        elif self.distro == "apache":
            raise Exception("Not Implemented Yet")
        render(source="kafka-broker.service.j2",
               target=target,
               user=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o644,
               context={
                   "kafka_broker_service_unit_overrides": kafka_broker_service_unit_overrides,
                   "kafka_broker_service_overrides": kafka_broker_service_overrides,
                   "kafka_broker_service_environment_overrides": kafka_broker_service_environment_overrides
               })

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.config_changed, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.cluster = KafkaBrokerCluster(self, 'cluster')
        self.zk = ZookeeperRequiresRelation(self, 'zookeeper')
        self.ks.set_default(zk_cert="")
        self.ks.set_default(zk_key="")
        os.makedirs("/var/ssl/private")
        self._generate_keystores()

    def _create_log_dirs(self):
        for d in self.config.get("log.dirs","").split(","):
            try:
                self.create_log_dir(data_log_dev=None,
                                    data_log_dir=d,
                                    data_log_fs=None,
                                    user=self.config["user"],
                                    group=self.config["group"])
            except:
                # folder already exists
                pass

    def _generate_keystores(self):
        if self.config["generate-root-ca"]:
            generateSelfSigned("/var/lib",
                               certname="zk-kafka-broker-root-ca",
                               user=self.config["user"],
                               group=self.config["group"],
                               mode=0o640)
            with open("/var/lib/zk-kafka-broker-root-ca.crt", "r") as f:
                self.ks.zk_cert = f.read()
                f.close()
            with open("/var/lib/zk-kafka-broker-root-ca.key", "r") as f:
                self.ks.zk_key = f.read()
                f.close()
            generateSelfSigned("/var/lib",
                               certname="ssl-kafka-broker-root-ca",
                               user=self.config["user"],
                               group=self.config["group"],
                               mode=0o640)
            with open("/var/lib/ssl-kafka-broker-root-ca.crt", "r") as f:
                self.ks.ssl_cert = f.read()
                f.close()
            with open("/var/lib/ssl-kafka-broker-root-ca.key", "r") as f:
                self.ks.ssl_key = f.read()
                f.close()
        else:
            # Certs already set either as configs or certificates relation
            self.ks.zk_cert  = get_zk_cert()
            self.ks.zk_key   = get_zk_key()
            self.ks.ssl_cert = get_ssl_cert()
            self.ks.ssl_key  = get_ssl_key()
        self.ks.ks_zookeeper_pwd = genRandomPassword()
        self.ks.ts_zookeeper_pwd = genRandomPassword()
        PKCS12CreateKeystore(
            self.config.get("keystore-zookeeper-path",
                            "/var/ssl/private/kafka_zookeeper_ks.jks"),
            self.ks.ks_zookeeper_pwd,
            self.get_zk_cert(),
            self.get_zk_key(),
            user=self.config["user"],
            group=self.config["group"],
            mode=0o640)
        PKCS12CreateKeystore(
            self.config.get("keystore-path",
                            "/var/ssl/private/kafka_ssl_ks.jks"),
            self.ks.ks_password,
            self.get_ssl_cert(),
            self.get_ssl_key(),
            user=self.config["user"],
            group=self.config["group"],
            mode=0o640)

    def _on_install(self, _):
        # TODO: Create /var/lib/kafka folder and all log dirs, set permissions
        packages = []
        if self.config.get("install_method") == "archive":
            self._install_tarball()
        else:
            if self.distro == "confluent":
                packages = CONFLUENT_PACKAGES
            else:
                raise Exception("Not Implemented Yet"
            super().install_packages('openjdk-11-headless', packages)

    def _rel_get_remote_units(self, rel_name):
        return self.framework.model.get_relation(rel_name).units

    def get_ssl_cert(self):
        if len(self.ks.ssl_cert) > 0:
            return self.ks.ssl_cert
        return base64.b64decode(self.config["ssl_cert"]).decode("ascii")

    def get_ssl_key(self):
        if len(self.ks.ssl_key) > 0:
            return self.ks.ssl_key
        return base64.b64decode(self.config["ssl_key"]).decode("ascii")

    def get_zk_cert(self):
        # TODO(pguimaraes): expand it to count with certificates relation or action cert/key
        if len(self.ks.zk_cert) > 0:
            return self.ks.zk_cert
        return base64.b64decode(self.config["ssl-zk-cert"]).decode("ascii")

    def get_zk_key(self):
        # TODO(pguimaraes): expand it to count with certificates relation or action cert/key
        if len(self.ks.zk_key) > 0:
            return self.ks.zk_key
        return base64.b64decode(self.config["ssl-zk-key"]).decode("ascii")

    def _generate_server_properties(self):
        # TODO: set confluent.security.event.logger.exporter.kafka.topic.replicas
        server_props = self.config.get("server-properties", "")
        server_props["log.dirs"] = self.config["log.dirs"]
        if os.environ.get("JUJU_AVAILABILITY_ZONE") and self.config["customize-failure-domain"]:
            server_props["broker.rack"] = os.environ.get("JUJU_AVAILABILITY_ZONE")
        replication_factor = self.config.get("replication-factor",3)
        server_props["offsets.topic.replication.factor"] = replication_factor
        if replication_factor > self.cluster.num_peers or
            (replication_factor > self.cluster.num_azs and self.config["customize-failure-domain"]):
            BlockedStatus("Not enough brokers (or AZs, if customize-failure-domain is set)")
            return
        server_props["transaction.state.log.min.isr"] = min(2, replication_factor)
        server_props["transaction.state.log.replication.factor"] = replication_factor
        if self.distro == "confluent":
            server_props["confluent.license.topic.replication.factor"] = replication_factor
            server_props["confluent.metadata.topic.replication.factor"] = replication_factor
            server_props["confluent.balancer.topic.replication.factor"] = replication_factor
        server_props["zookeeper.connect"] = self.zk.get_zookeeper_list
        server_props = { **server_props, **self.cluster.listeners }
        # TODO: Enable rest proxy if we have RBAC: https://github.com/confluentinc/cp-ansible/blob/b711fc9e3b43d2069a9ac8b13177e7f2a07c7bfb/VARIABLES.md
        server_props["kafka_broker_rest_proxy_enabled"] = False

        # Zookeeper options:
        server_props["zookeeper.connect"] = self.zk.get_zookeer_list
        server_props["zookeeper.set.acl"] = zookeeper.clientCnxnSocket()
        if self.zk.is_mTLS_enabled():
            # TLS client properties uses the same variables
            # Rendering as a part of server properties
            client_props = {}
            client_props["zookeeper.clientCnxnSocket"] = "org.apache.zookeeper.ClientCnxnSocketNetty"
            client_props["zookeeper.ssl.client.enable"] = "true"
            client_props["zookeeper.ssl.keystore.location"] = \
                self.config.get("keystore-zookeeper-path",
                                "/var/ssl/private/kafka_zookeeper_ks.jks")
            self.ks.ks_zookeeper_pwd = genRandomPassword()
            client_props["zookeeper.ssl.keystore.password"] = self.ks.ks_zookeeper_pwd
            client_props["zookeeper.ssl.truststore.location"] = \
                self.config.get("truststore-zookeeper-path",
                                "/var/ssl/private/kafka_zookeeper_ts.jks")
            self.ks.ts_zookeeper_pwd = genRandomPassword()
            client_props["zookeeper.ssl.truststore.password"] = self.ks.ts_zookeeper_pwd
            # Set the SSL mTLS config on the relation
            self.zk.set_mTLS_auth(self.get_zk_cert(),
                                  client_props["zookeeper.ssl.truststore.location"],
                                  self.ks.ts_password)
            # Attaching client_props to server_props config
            server_props = {**server_props, **client_props}
            render(source="zookeeper-tls-client.properties.j2",
                   target="/etc/kafka/zookeeper-tls-client.properties",
                   user=self.config.get('user'),
                   group=self.config.get("group"),
                   perms=0o640,
                   context={
                       "client_props": client_props
                   })
        # Back to server.properties, render it
        render(source="server.properties.j2",
               target="/etc/kafka/server.properties",
               user=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o640,
               context={
                   "server_props": server_props
               })

    def is_sasl_enabled(self):
        if self.is_sasl_kerberos_enabled() or \
           self.is_sasl_oauthbearer_enabled() or \
           self.is_sasl_scram_enabled() or \
           self.is_sasl_plain_enabled() or \
           self.is_sasl_delegate_token_enabled() or \
           self.is_sasl_ldap_enabled():
            return True
        return False

    def _generate_client_properties(self):
        # TODO: it seems we need this just when it comes to client encryption
        # https://github.com/confluentinc/cp-ansible/blob/b711fc9e3b43d2069a9ac8b13177e7f2a07c7bfb/roles/confluent.variables/filter_plugins/filters.py#L159
        client_props = self.config["client-properties"] or {}
        if self.is_sasl_enabled():
            client_props["sasl.jaas.config"] = self.config.get("sasl-jaas-config","")
        if self.is_sasl_kerberos_enabled():
            client_prpos["sasl.mechanism"] = "GSSAPI"
            client_props["sasl.kerberos.service.name"] = self.config.get("sasl-kbros-service", "HTTP")
        if self.is_ssl_enabled():
            client_props["ssl.keystore.location"] = \
                self.config.get("keystore-path",
                                "/var/ssl/private/kafka_ks.jks")
            client_props["ssl.keystore.password"] = self.ks.ks_password
            client_props["ssl.truststore.location"] = \
                self.config.get("truststore-path",
                                "/var/ssl/private/kafka_ts.jks")
            client_props["ssl.truststore.password"] = self.ks.ts_password
        render(source="client.properties.j2",
               target="/etc/kafka/client.properties",
               user=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o640,
               context={
                   "client_props": client_props
               })

#    def _generate_zk_tls_client_properties(self):
#        client_props = {}
#        if not self.zk.is_mTLS_enabled():
#            return
#        client_props["zookeeper.clientCnxnSocket"] = "org.apache.zookeeper.ClientCnxnSocketNetty"
#        client_props["zookeeper.ssl.client.enable"] = "true"
#        client_props["zookeeper.ssl.keystore.location"] = \
#            self.config.get("keystore-zookeeper-path",
#                            "/var/ssl/private/kafka_zookeeper_ks.jks")
#        client_props["zookeeper.ssl.keystore.password"] = self.ks.ks_password
#        client_props["zookeeper.ssl.truststore.location"] = \
#            self.config.get("truststore-zookeeper-path",
#                            "/var/ssl/private/kafka_zookeeper_ts.jks")
#        client_props["zookeeper.ssl.truststore.password"] = self.ks.ts_password
#        render(source="zookeeper-tls-client.properties.j2",
#               target="/etc/kafka/zookeeper-tls-client.properties",
#               user=self.config.get('user'),
#               group=self.config.get("group"),
#               perms=0o640,
#               context={
#                   "client_props": client_props
#               })


    def _on_config_changed(self, _):
        # Note: you need to uncomment the example in the config.yaml file for this to work (ensure
        # to not just leave the example, but adapt to your configuration needs)
        self._generate_keystores()
        self._generate_server_properties()
        self._generate_client_properties()
#        self._generate_zk_tls_client_properties()
        target = None
        if self.distro == "confluent":
            target = "/etc/systemd/system/" + \
                     "confluent-server.service.d/override.conf"
        elif self.distro == "apache":
            raise Exception("Not Implemented Yet")
        self.render_service_override_file(target)
        # Apply sysctl



if __name__ == "__main__":
    main(KafkaBrokerCharm)
