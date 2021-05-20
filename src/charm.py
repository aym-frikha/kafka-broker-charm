#!/usr/bin/env python3
# Copyright 2021 pguimaraes
# See LICENSE file for licensing details.

import base64
import logging
import os
import socket
import yaml
import json

from ops.main import main
from ops.model import (
    MaintenanceStatus,
    ActiveStatus,
    BlockedStatus
)

from charmhelpers.core.templating import render

from charmhelpers.core.host import (
    service_running
)

from wand.apps.kafka import (
    KafkaJavaCharmBase,
    KafkaCharmBaseMissingConfigError
)
from cluster import KafkaBrokerCluster
from wand.apps.relations.zookeeper import ZookeeperRequiresRelation
from wand.apps.relations.kafka_relation_base import (
    KafkaRelationBaseNotUsedError,
    KafkaRelationBaseTLSNotSetError
)

from wand.apps.relations.tls_certificates import (
    TLSCertificateRequiresRelation,
    TLSCertificateDataNotFoundInRelationError,
    TLSCertificateRelationNotPresentError
)

from wand.security.ssl import (
    setFilePermissions,
    genRandomPassword,
    generateSelfSigned,
    PKCS12CreateKeystore,
    CreateTruststore
)
from wand.apps.relations.kafka_listener import (
    KafkaListenerProvidesRelation,
    KafkaListenerRelationEmptyListenerDictError
)
from wand.apps.relations.kafka_mds import (
    KafkaMDSProvidesRelation
)
from wand.contrib.coordinator import (
    RestartCharmEvent,
    OpsCoordinator
)

logger = logging.getLogger(__name__)

# Given: https://docs.confluent.io/current/ \
#        installation/cp-ansible/ansible-configure.html
# Setting confluent-server by default
# confluent-cli gives access to confluent and IAM management
CONFLUENT_PACKAGES = [
  "confluent-cli",
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


class KafkaBrokerCharmMDSNotSupportedError(Exception):

    def __init__(self, distro):
        message = "MDS is not supported on " + \
                  "{} distribution, only Confluent".format(distro)
        super().__init__(message)


class KafkaBrokerCharm(KafkaJavaCharmBase):
    on = RestartCharmEvent()

    def _install_tarball(self):
        # Use _generate_service_files here
        raise Exception("Not Implemented Yet")

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.cluster_relation_joined,
                               self._on_cluster_relation_joined)
        self.framework.observe(self.on.cluster_relation_changed,
                               self._on_cluster_relation_changed)
        self.framework.observe(self.on.zookeeper_relation_joined,
                               self._on_zookeeper_relation_joined)
        self.framework.observe(self.on.zookeeper_relation_changed,
                               self._on_zookeeper_relation_changed)
        self.framework.observe(self.on.certificates_relation_joined,
                               self.on_certificates_relation_joined)
        self.framework.observe(self.on.certificates_relation_changed,
                               self.on_certificates_relation_changed)
        self.framework.observe(self.on.update_status,
                               self.on_update_status)
        self.framework.observe(self.on.mds_relation_joined,
                               self.on_mds_relation_joined)
        self.framework.observe(self.on.mds_relation_changed,
                               self.on_mds_relation_changed)
        self.framework.observe(self.on.listeners_relation_joined,
                               self.on_listeners_relation_joined)
        self.framework.observe(self.on.listeners_relation_changed,
                               self.on_listeners_relation_changed)
        self.framework.observe(self.on.restart_event,
                               self.on_restart_event)
        self.framework.observe(self.on.upload_keytab_action,
                               self.on_upload_keytab_action)
        self.cluster = KafkaBrokerCluster(self, 'cluster',
                                          self.config.get("cluster-count", 3))
        self.zk = ZookeeperRequiresRelation(self, 'zookeeper')
        self.certificates = \
            TLSCertificateRequiresRelation(self, 'certificates')
        self.ks.set_default(zk_cert="")
        self.ks.set_default(zk_key="")
        self.ks.set_default(ssl_cert="")
        self.ks.set_default(ssl_key="")
        self.ks.set_default(ts_zookeeper_pwd=genRandomPassword())
        self.ks.set_default(ks_zookeeper_pwd=genRandomPassword())
        self.ks.set_default(changed_params="{}")
        self.listener = KafkaListenerProvidesRelation(self, 'listeners')
        self.mds = KafkaMDSProvidesRelation(self, 'mds')
        self.ks.set_default(config_state="{}")

    def on_upload_keytab_action(self, event):
        try:
            self._upload_keytab_base64(
                event.params["keytab"], filename="kafka_broker.keytab")
        except Exception as e:
            # Capture any exceptions and return them via action
            event.fail("Failed with: {}".format(str(e)))
            return
        self._on_config_changed(event)
        event.set_results({"keytab": "Uploaded!"})

    def on_restart_event(self, event):
        if event.restart():
            self.ks.config_state = event.ctx
        else:
            event.defer()

    def on_certificates_relation_joined(self, event):
        self.certificates.on_tls_certificate_relation_joined(event)
        self._on_config_changed(event)

    def on_certificates_relation_changed(self, event):
        self.certificates.on_tls_certificate_relation_changed(event)
        self._on_config_changed(event)

    def on_listeners_relation_joined(self, event):
        if not self._cert_relation_set(event, self.listener):
            return
        self.listener.on_listener_relation_joined(event)
        self._on_config_changed(event)

    def on_listeners_relation_changed(self, event):
        if not self._cert_relation_set(event, self.listener):
            return
        self.listener.on_listener_relation_changed(event)
        self._on_config_changed(event)

    def on_mds_relation_joined(self, event):
        # TODO: Implement
        return

    def on_mds_relation_changed(self, event):
        # TODO: Implement
        return

    def on_update_status(self, event):
        # Check if the locks must be handled or not
        coordinator = OpsCoordinator()
        coordinator.handle_locks(self.unit)
        if not service_running(self.service):
            self.model.unit.status = \
                BlockedStatus("{} not running".format(self.service))
            return
        self.model.unit.status = \
            ActiveStatus("{} is running".format(self.service))

    def _on_cluster_relation_joined(self, event):
        if not self._cert_relation_set(event, self.cluster):
            return
        self.cluster.user = self.config.get("user", "")
        self.cluster.group = self.config.get("group", "")
        self.cluster.mode = 0o640
        try:
            self.cluster.on_cluster_relation_joined(event)
        except KafkaRelationBaseNotUsedError as e:
            # Relation not been used any other application, move on
            logger.info(str(e))
        except KafkaRelationBaseTLSNotSetError as e:
            event.defer()
            self.model.unit.status = BlockedStatus(str(e))
        self._on_config_changed(event)

    def _on_cluster_relation_changed(self, event):
        if not self._cert_relation_set(event, self.cluster):
            return
        self.cluster.user = self.config.get("user", "")
        self.cluster.group = self.config.get("group", "")
        self.cluster.mode = 0o640
        try:
            self.cluster.on_cluster_relation_changed(event)
        except KafkaRelationBaseNotUsedError as e:
            # Relation not been used any other application, move on
            logger.info(str(e))
        except KafkaRelationBaseTLSNotSetError as e:
            event.defer()
            self.model.unit.status = BlockedStatus(str(e))
        self._on_config_changed(event)

    def _on_zookeeper_relation_joined(self, event):
        if not self._cert_relation_set(event, self.zk):
            return
        self.zk.user = self.config.get("user", "")
        self.zk.group = self.config.get("group", "")
        self.zk.mode = 0o640
        try:
            self.zk.on_zookeeper_relation_joined(event)
        except KafkaRelationBaseNotUsedError as e:
            # Relation not been used any other application, move on
            logger.info(str(e))
        except KafkaRelationBaseTLSNotSetError as e:
            event.defer()
            self.model.unit.status = BlockedStatus(str(e))
        self._on_config_changed(event)

    def _on_zookeeper_relation_changed(self, event):
        if not self._cert_relation_set(event, self.zk):
            return
        self.zk.user = self.config.get("user", "")
        self.zk.group = self.config.get("group", "")
        self.zk.mode = 0o640
        try:
            self.zk.on_zookeeper_relation_changed(event)
        except KafkaRelationBaseNotUsedError as e:
            # Relation not been used any other application, move on
            logger.info(str(e))
        except KafkaRelationBaseTLSNotSetError as e:
            event.defer()
            self.model.unit.status = BlockedStatus(str(e))
        self._on_config_changed(event)

    def _cert_relation_set(self, event, rel=None):
        # generate cert request if tls-certificates available
        # rel may be set to None in cases such as config-changed
        # or install events. In these cases, the goal is to run
        # the validation at the end of this method
        if rel:
            if self.certificates.relation and rel.relation:
                sans = [
                    rel.binding_addr,
                    rel.advertise_addr,
                    rel.hostname,
                    socket.gethostname()
                ]
                # Common name is always CN as this is the element
                # that organizes the cert order from tls-certificates
                self.certificates.request_server_cert(
                    cn=rel.binding_addr,
                    sans=sans)
            logger.info("Either certificates "
                        "relation not ready or not set")
        # This try/except will raise an exception if tls-certificate
        # is set and there is no certificate available on the relation yet.
        # That will also cause the
        # event to be deferred, waiting for certificates relation to finish
        # If tls-certificates is not set, then the try will run normally,
        # either marking there is no certificate configuration set or
        # concluding the method.
        try:
            if (not self.get_ssl_cert() or not self.get_ssl_key() or
               not self.get_zk_cert() or not self.get_zk_key()):
                self.model.unit.status = \
                    BlockedStatus("Waiting for certificates "
                                  "relation or option")
                logger.info("Waiting for certificates relation "
                            "to publish data")
                return False
        # These excepts will treat the case tls-certificates relation is used
        # but the relation is not ready yet
        # KeyError is also a possibility, if get_ssl_cert is called before any
        # event that actually submits a request for a cert is done
        except (TLSCertificateDataNotFoundInRelationError,
                TLSCertificateRelationNotPresentError,
                KeyError):
            self.model.unit.status = \
                BlockedStatus("There is no certificate option or "
                              "relation set, waiting...")
            logger.warning("There is no certificate option or "
                           "relation set, waiting...")
            if event:
                event.defer()
            return False
        return True

    def _generate_keystores(self):
        # If we will auto-generate the root ca
        # and at least one of the certs or keys is not yet set,
        # then we can proceed and regenerate it.
        if self.config["generate-root-ca"] and \
            (len(self.ks.ssl_cert) > 0 and
             len(self.ks.ssl_key) > 0 and
             len(self.ks.zk_cert) > 0 and
             len(self.ks.zk_key) > 0):
            return
        if self.config["generate-root-ca"]:
            self.ks.ssl_cert, self.ks.ssl_key = \
                generateSelfSigned(self.unit_folder,
                                   certname="zk-kafka-broker-root-ca",
                                   user=self.config["user"],
                                   group=self.config["group"],
                                   mode=0o640)
            self.ks.zk_cert, self.ks.zk_key = \
                generateSelfSigned(self.unit_folder,
                                   certname="ssl-kafka-broker-root-ca",
                                   user=self.config["user"],
                                   group=self.config["group"],
                                   mode=0o640)
        else:
            # Check if the certificates remain the same
            if self.ks.zk_cert == self.get_zk_cert() and \
                    self.ks.zk_key == self.get_zk_key() and \
                    self.ks.ssl_cert == self.get_ssl_cert() and \
                    self.ks.ssl_key == self.get_ssl_key():
                # Yes, they do, leave this method as there is nothing to do.
                return
            # Certs already set either as configs or certificates relation
            self.ks.zk_cert = self.get_zk_cert()
            self.ks.zk_key = self.get_zk_key()
            self.ks.ssl_cert = self.get_ssl_cert()
            self.ks.ssl_key = self.get_ssl_key()
        if (len(self.ks.zk_cert) > 0 and len(self.ks.zk_key) > 0):
            self.ks.ks_zookeeper_pwd = genRandomPassword()
            filename = genRandomPassword(6)
            PKCS12CreateKeystore(
                self.get_zk_keystore(),
                self.ks.ks_zookeeper_pwd,
                self.get_zk_cert(),
                self.get_zk_key(),
                user=self.config["user"],
                group=self.config["group"],
                mode=0o640,
                openssl_chain_path="/tmp/" + filename + ".chain",
                openssl_key_path="/tmp/" + filename + ".key",
                openssl_p12_path="/tmp/" + filename + ".p12",
                ks_regenerate=self.config.get(
                                  "regenerate-keystore-truststore", False))
        if len(self.ks.ssl_cert) > 0 and \
           len(self.ks.ssl_key) > 0:
            self.ks.ks_password = genRandomPassword()
            filename = genRandomPassword(6)
            PKCS12CreateKeystore(
                self.get_ssl_keystore(),
                self.ks.ks_password,
                self.get_ssl_cert(),
                self.get_ssl_key(),
                user=self.config["user"],
                group=self.config["group"],
                mode=0o640,
                openssl_chain_path="/tmp/" + filename + ".chain",
                openssl_key_path="/tmp/" + filename + ".key",
                openssl_p12_path="/tmp/" + filename + ".p12",
                ks_regenerate=self.config.get(
                                  "regenerate-keystore-truststore", False))

    def _on_install(self, event):
        self.model.unit.status = MaintenanceStatus("Installing packages...")
        super()._on_install(event)
        packages = []
        if self.config.get("install_method") == "archive":
            self._install_tarball()
        else:
            if self.distro == "confluent":
                packages = CONFLUENT_PACKAGES
            else:
                raise Exception("Not Implemented Yet")
            super().install_packages('openjdk-11-headless', packages)
        # The logic below avoid an error such as more than one entry
        # In this case, we will pick the first entry
        data_log_fs = \
            list(yaml.safe_load(
                     self.config.get("data-log-dir", "")).items())[0][0]
        data_log_dir = \
            list(yaml.safe_load(
                     self.config.get("data-log-dir", "")).items())[0][1]
        self.create_log_dir(self.config["data-log-device"],
                            data_log_dir,
                            data_log_fs,
                            self.config.get("user",
                                            "cp-kafka"),
                            self.config.get("group",
                                            "confluent"),
                            self.config.get("fs-options", None))
        self._on_config_changed(event)

    def _check_if_ready_to_start(self, ctx):
        if not self.cluster.is_ready:
            self.model.unit.status = \
                BlockedStatus("Waiting for other cluster units")
            return False
        # ctx can be a string or dict, then check and convert accordingly
        c = json.dumps(ctx) if isinstance(ctx, dict) else ctx
        if c == self.ks.config_state:
            logger.debug("Current state: {}, saved state: {}".format(
                c, self.ks.config_state
            ))
            return False
        return True

    def _rel_get_remote_units(self, rel_name):
        return self.framework.model.get_relation(rel_name).units

    def get_ssl_cert(self):
        if self.config["generate-root-ca"]:
            return self.ks.ssl_cert
        if len(self.config.get("ssl_cert")) > 0 and \
           len(self.config.get("ssl_key")) > 0:
            return base64.b64decode(self.config["ssl_cert"]).decode("ascii")
        certs = self.certificates.get_server_certs()
        c = certs[self.cluster.binding_addr]["cert"] + \
            self.certificates.get_chain()
        logger.debug("SSL Certificate chain from "
                     "tls-certificates: {}".format(c))
        return c

    def get_ssl_key(self):
        if self.config["generate-root-ca"]:
            return self.ks.ssl_key
        if len(self.config.get("ssl_cert")) > 0 and \
           len(self.config.get("ssl_key")) > 0:
            return base64.b64decode(self.config["ssl_key"]).decode("ascii")
        certs = self.certificates.get_server_certs()
        k = certs[self.cluster.binding_addr]["key"]
        return k

    def get_ssl_keystore(self):
        path = self.config.get("keystore-path", "")
        return path

    def get_ssl_truststore(self):
        path = self.config.get("truststore-path", "")
        return path

    def get_zk_keystore(self):
        path = self.config.get("keystore-zookeeper-path", "")
        return path

    def get_zk_truststore(self):
        path = self.config.get("truststore-zookeeper-path", "")
        return path

    def get_zk_cert(self):
        if self.config["generate-root-ca"]:
            return self.ks.zk_cert
        if len(self.config.get("ssl_cert")) > 0 and \
           len(self.config.get("ssl_key")) > 0:
            return base64.b64decode(
                self.config["ssl_cert"]).decode("ascii")
        certs = self.certificates.get_server_certs()
        c = certs[self.zk.binding_addr]["cert"] + \
            self.certificates.get_chain()
        logger.debug("SSL Certificate chain from "
                     "tls-certificates: {}".format(c))
        return c

    def get_zk_key(self):
        if self.config["generate-root-ca"]:
            return self.ks.zk_key
        if len(self.config.get("ssl_cert")) > 0 and \
           len(self.config.get("ssl_key")) > 0:
            return base64.b64decode(
                self.config["ssl_key"]).decode("ascii")
        certs = self.certificates.get_server_certs()
        k = certs[self.zk.binding_addr]["key"]
        return k

    def get_oauth_token_cert(self):
        # TODO: implement cert/key generation and
        # management through this getters
        raise Exception("Not Implemented Yet")

    def get_oauth_token_key(self):
        # TODO: implement cert/key generation and
        # management through this getters
        raise Exception("Not Implemented Yet")

    def _generate_server_properties(self, event):
        # TODO: set
        # confluent.security.event.logger.exporter.kafka.topic.replicas
        self.model.unit.status = \
            MaintenanceStatus("Starting server.properties")
        server_props = \
            yaml.safe_load(self.config.get("server-properties", "")) or {}
        # https://docs.confluent.io/platform/current/installation/license.html
        if self.get_license_topic() and len(self.get_license_topic()) > 0:
            server_props["confluent.license"] = self.get_license_topic()
        server_props["log.dirs"] = \
            list(yaml.safe_load(
                     self.config.get("data-log-dir", "")).items())[0][1]
        logger.info("Selected {} for "
                    "log.dirs".format(server_props["log.dirs"]))

        if (os.environ.get("JUJU_AVAILABILITY_ZONE", None) and
                self.config.get("customize-failure-domain", False)):
            server_props["broker.rack"] = \
                os.environ.get("JUJU_AVAILABILITY_ZONE")
            logger.info("Failure domains enabled, broker.rack={}".format(
                server_props["broker.rack"]))
        # Resolve replication factors
        replication_factor = self.config.get("replication-factor", 3)
        server_props["offsets.topic.replication.factor"] = replication_factor
        if replication_factor > self.cluster.num_peers or \
           (replication_factor > self.cluster.num_azs and
                self.config["customize-failure-domain"]):
            self.model.unit.status = \
                BlockedStatus("Not enough brokers " +
                              "(or AZs, if customize-failure-domain is set)")
            return

        # CLUSTER SIZE CHECK
        logger.info("Check if cluster has the minimum count needed")
        self.cluster.min_units = self.config.get("cluster-count", 3)
        if self.config.get("cluster-count", 3) > 1 and \
           not self.cluster.relations:
            logger.debug("Cluster-count > 1 but cluster.relation "
                         "object does not exist")
            self.model.unit.status = \
                BlockedStatus("Cluster detected, waiting for {} units to"
                              " come up". format(
                                  self.config.get("cluster-count")))
            return
        if self.config.get("cluster-count", 3) > \
           len(self.cluster.all_units(self.cluster.relations)):
            all_u = len(self.cluster.all_units(self.cluster.relations))
            logger.debug("Cluster.relation obj exists but "
                         "all_units return {}".format(all_u))
            self.model.unit.status = \
                BlockedStatus("Cluster detected, waiting for {} units to"
                              " come up". format(
                                  self.config.get("cluster-count")))
            return
        server_props["inter.broker.listener.name"] = "BROKER"

        # Last configs: set replication factors
        server_props["transaction.state.log.min.isr"] = \
            min(2, replication_factor)
        server_props["transaction.state.log.replication.factor"] = \
            replication_factor
        if self.distro == "confluent":
            server_props["confluent.license.topic.replication.factor"] = \
                replication_factor
            server_props["confluent.metadata.topic.replication.factor"] = \
                replication_factor
            server_props["confluent.balancer.topic.replication.factor"] = \
                replication_factor
        logger.info("Finished setting replication_factor parameters")

        # TODO: Enable rest proxy if we have RBAC:
        # https://github.com/confluentinc/cp-ansible/blob/ \
        #     b711fc9e3b43d2069a9ac8b13177e7f2a07c7bfb/VARIABLES.md
#        server_props["kafka_broker_rest_proxy_enabled"] = False

        # Cluster certificate: this is set using BROKER listener
        if (len(self.get_ssl_cert()) > 0 and
           len(self.get_ssl_key()) > 0) and self.get_ssl_truststore():
            logger.info("Setting SSL for client connections")
            user = self.config.get("user", "")
            group = self.config.get("group", "")
            # Manage the cluster certs first, set_ssl_cert
            # checks if relation exists
            self.cluster.set_ssl_cert(self.get_ssl_cert())
            extra_certs = self.cluster.get_all_certs()
            logger.debug("Extra certificates "
                         "for cluster: {}".format(extra_certs))
            # There are 3x possible situations to manage certs:
            # 1) listener relation exists: push certs there
            # 2) cluster only relation exists: use it to generate certs
            # 3) None of the above: it is a single node
            if self.listener.relations:
                logger.info("Listener relation found: manage certs")
                self.listener.set_TLS_auth(
                    self.get_ssl_cert(),
                    self.get_ssl_truststore(),
                    self.ks.ts_password,
                    user, group, 0o640,
                    extra_certs=extra_certs)
            elif self.cluster.relation:
                logger.info("Listener relation not found"
                            " but cluster is present: manage certs")
                extra_certs = self.cluster.get_all_certs()
                extra_certs.append(self.get_ssl_cert())
                self.listener.user = self.config["user"]
                self.listener.group = self.config["group"]
                self.listener.mode = 0o640
                self.listener.ts_path = self.get_ssl_truststore()
                self.listener.ts_pwd = self.ks.ts_password
#                self.cluster._get_all_tls_certs()
                CreateTruststore(self.get_ssl_truststore(),
                                 self.ks.ts_password,
                                 extra_certs,
                                 ts_regenerate=True,
                                 user=self.config["user"],
                                 group=self.config["group"],
                                 mode=0o640)
            else:
                logger.info("Neither Listener nor Cluster relations "
                            "create the truststore manually.")
                CreateTruststore(self.get_ssl_truststore(),
                                 self.ks.ts_password,
                                 [self.get_ssl_cert()],
                                 ts_regenerate=True,
                                 user=self.config["user"],
                                 group=self.config["group"],
                                 mode=0o640)

        listeners = {}
        listener_opts = {}
        if self.unit.is_leader():
            # Listener logic
            # Convert it to dict and then back to string after the loop below
            listeners_d = json.loads(self.listener.get_unit_listener(
                self.get_ssl_keystore(),
                self.ks.ks_password,
                get_default=True,
                clientauth=self.config.get("clientAuth", False)))
            # Set EXTERNAL and BROKER listeners'
            for i in [("broker", "cluster-auth-method"),
                      ("external", "external-auth-method")]:
                lst = listeners_d[i[0]]
                conf = i[1]
                if self.config[conf].lower() == "gssapi" and \
                   self.is_sasl_kerberos_enabled():
                    principal = "{}/{}@{}".format(
                        self.config["kerberos-protocol"],
                        self.config["kerberos-domain"],
                        self.config["kerberos-realm"])
                    lst["secprot"] = "SASL_SSL"
                    lst["SASL"] = {
                        "protocol": "GSSAPI",
                        "kerberos-protocol": self.config["kerberos-protocol"],
                        "kerberos-principal": principal,
                        "kerberos-realm": self.config["kerberos-realm"],
                    }
                elif self.config[conf].lower() == "bearer" and \
                        self.is_sasl_oauthbearer_enabled():
                    lst["SASL"]["protocol"] = "OAUTHBEARER"
                    lst["secprot"] = "SASL_SSL"
                    if self.config["oauth-token-verify"]:
                        lst["SASL"]["publicKeyPath"] = \
                            self.get_oauth_token_cert()
                    if self.distro == "confluent":
                        # Add Confluent-specific settings:
                        lst["SASL"]["confluent"] = {
                            "login.callback": \
                                "io.confluent.kafka.server.plugins.auth.token.TokenBearerServerLoginCallbackHandler", # noqa
                            "server.callback": \
                                "io.confluent.kafka.server.plugins.auth.token.TokenBearerValidatorCallbackHandler" # noqa
                        }
                elif len(self.config[conf]) > 0:
                    # TODO: implement this mechanism
                    raise Exception("Not implemented yet")
            listeners = json.dumps(listeners_d)
            logger.debug("Listener changed auth"
                         "methods to: {}".format(listeners))
            # update peers
            self.cluster.set_listeners(listeners)
        else:
            listeners = self.cluster.get_listener_template()
        listener_opts = self.listener._generate_opts(
            listeners,
            self.get_ssl_keystore(),
            self.ks.ks_password,
            get_default=True,
            clientauth=self.config.get("clientAuth", False))
        if len(self.listener.get_sasl_mechanisms_list()) > 0:
            server_props["sasl.enabled.mechanisms"] = ",".join(
                self.listener.get_sasl_mechanism_list())
            server_props["sasl.kerberos.service.name"] = "HTTP"
            if "protocol" in listeners["broker"]["SASL"]:
                server_props["sasl.mechanism."
                             "inter.broker.protocol"] = \
                    listeners["broker"]["SASL"]

        # Each unit sets its own data
        self.listener.set_bootstrap_data(listeners)
        logger.debug("Found listeners: {}".format(listeners))
        server_props = {**server_props, **listener_opts}

        # Metadata service relation
        if self.mds.relations and self.distro != "confluent":
            raise KafkaBrokerCharmMDSNotSupportedError(self.distro)
        if self.mds.relations:
            # MDS will use the same set of certificates for listeners
            protocol = "https" if len(self.get_ssl_key()) > 0 else "http"
            server_props["confluent.metadata.server.advertised.listeners"] = \
                "{}://{}:8090".format(protocol, self.listener.hostname)
            server_props["confluent.metadata."
                         "server.authentication.method"] = "BEARER"
            server_props["confluent.metadata.server.listeners"] = \
                "{}://{}:8090".format(protocol, self.listener.hostname)
            server_props["confluent.metadata."
                         "server.ssl.key.password"] = self.ks.ks_password
            server_props["confluent.metadata."
                         "server.ssl.keystore.location"] = \
                self.get_ssl_keystore()
            server_props["confluent.metadata."
                         "server.ssl.keystore.password"] = \
                self.ks.ks_password
            server_props["confluent.metadata."
                         "server.ssl.truststore.location"] = \
                self.get_ssl_truststore()
            server_props["confluent.metadata."
                         "server.ssl.truststore.password"] = \
                self.ks.ts_password
            if self.config["oauth-token-verify"]:
                server_props["confluent.metadata.server.token.auth.enable"] = \
                    self.config["oauth-token-verify"]
                server_props["confluent.metadata.server.public.key.path"] = \
                    self.get_oauth_token_cert()
                server_props["confluent.metadata.server.token.key.path"] = \
                    self.get_oauth_token_key()
                server_props["confluent.metadata."
                             "server.token.max.lifetime.ms"] = \
                    "3600000"
                server_props["confluent.metadata."
                             "server.token.signature.algorithm"] = "RS256"
                server_props["confluent.metadata."
                             "topic.replication.factor"] = "3"

        # Zookeeper options:
        self.model.unit.status = \
            MaintenanceStatus("render_server_properties: Start ZK configs")
        if self.get_zk_cert() and self.get_zk_key():
            self.zk.user = self.config.get("user", "root")
            self.zk.group = self.config.get("group", "root")
            self.zk.mode = 0o640
            try:
                self.zk.set_mTLS_auth(
                    self.get_zk_cert(),
                    self.get_zk_truststore(),
                    self.ks.ts_zookeeper_pwd)
            except KafkaRelationBaseNotUsedError as e:
                # Relation not been used any other application, move on
                logger.info(str(e))
            except KafkaRelationBaseTLSNotSetError as e:
                self.model.unit.status = BlockedStatus(str(e))

        if len(self.config.get("authorizer-class-name", "")) > 0:
            server_props["authorizer.class.name"] = \
                self.config.get("authorizer-class-name", "")

        server_props["zookeeper.connect"] = self.zk.get_zookeeper_list
        server_props["zookeeper.set.acl"] = self.zk.is_sasl_enabled()

        if self.zk.is_TLS_enabled():
            logger.info("Zookeeper SSL client enabled, "
                        "writing tls-client.properties")
            # TLS client properties uses the same variables
            # Rendering as a part of server properties
            client_props = {}
            client_props["zookeeper.clientCnxnSocket"] = \
                "org.apache.zookeeper.ClientCnxnSocketNetty"
            client_props["zookeeper.ssl.client.enable"] = "true"
            client_props["zookeeper.ssl.keystore.location"] = \
                self.get_zk_keystore()
            client_props["zookeeper.ssl.keystore.password"] = \
                self.ks.ks_zookeeper_pwd
            # Use the option, instead of get_zk_truststore method.
            # That will tell if the operator wants or
            # not a custom truststore defined
            if len(self.config.get("truststore-zookeeper-path", "")) > 0:
                client_props["zookeeper.ssl.truststore.location"] = \
                    self.get_zk_truststore()
                client_props["zookeeper.ssl.truststore.password"] = \
                    self.ks.ts_zookeeper_pwd
            else:
                logger.debug("Truststore not set for zookeeper relation, "
                             "Java truststore will be used instead")
            logger.debug("Options for TLS client: "
                         "{}".format(",".join(client_props)))
            render(source="zookeeper-tls-client.properties.j2",
                   target="/etc/kafka/zookeeper-tls-client.properties",
                   owner=self.config.get('user'),
                   group=self.config.get("group"),
                   perms=0o640,
                   context={
                       "client_props": client_props
                   })
            server_props = {**server_props, **client_props}
        logger.debug("Finished server.properties, options: "
                     "{}".format(",".join(server_props)))
        # Back to server.properties, render it
        render(source="server.properties.j2",
               target="/etc/kafka/server.properties",
               owner=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o640,
               context={
                   "server_props": server_props
               })
        return server_props

    def _generate_client_properties(self):
        logger.info("Generating client.properties")
        self.model.unit.status = \
            MaintenanceStatus("Generating client.properties")
        client_props = yaml.safe_load(self.config["client-properties"]) or {}
#        if self.is_sasl_enabled():
#            client_props["sasl.jaas.config"] = \
#                self.config.get("sasl-jaas-config", "")
        if self.zk.is_sasl_kerberos_enabled():
            client_props["sasl.mechanism"] = "GSSAPI"
            client_props["sasl.kerberos.service.name"] = \
                self.config.get("sasl-kbros-service", "HTTP")
            client_props["security.protocol"] = "SASL_SSL"
            sasl_config = \
                "com.sun.security.auth.module.Krb5LoginModule required " + \
                "useKeyTab=true " + \
                'keyTab="/etc/security/keytabs/{}" ' + \
                'storeKey=true ' + \
                'useTicketCache=false ' + \
                'principal="{}";'
            client_props["sasl.jaas.config"] = \
                sasl_config.format(self.keytab, self.kerberos_principal)
        if self.is_ssl_enabled():
#            client_props["ssl.keystore.location"] = \
#                self.config.get("keystore-path",
#                                "/var/ssl/private/kafka_ks.jks")
#            client_props["ssl.keystore.password"] = self.ks.ks_password
            if len(self.get_zk_truststore()) > 0:
                client_props["ssl.truststore.location"] = \
                     self.get_zk_truststore()
                client_props["ssl.truststore.password"] = \
                    self.ks.ts_zookeeper_pwd
            else:
                logger.debug("truststore-path not set, "
                             "using Java own truststore instead")
        logger.debug("Finished client.properties, options are: "
                     "{}".format(",".join(client_props)))
        render(source="client.properties.j2",
               target="/etc/kafka/client.properties",
               owner=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o640,
               context={
                   "client_props": client_props
               })
        return client_props

    def _get_service_name(self):
        if self.distro == "confluent":
            self.service = "confluent-server"
        else:
            self.service = "kafka"
        return self.service

    def _render_jaas_conf(self, jaas_path="/etc/kafka/jaas.conf"):
        content = ""
        if self.is_sasl_kerberos_enabled():
            krb = """KafkaServer {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/security/keytabs/{}"
    storeKey=true
    useTicketCache=false
    principal="{}";
}};
""".format(self.keytab, self.kerberos_principal) # noqa
            content += krb
            if self.zk.is_sasl_kerberos_enabled():
                content += """Client {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/security/keytabs/{}"
    storeKey=true
    useTicketCache=false
    principal="{}";
}};
""".format(self.keytab, self.kerberos_principal) # noqa
        self.set_folders_and_permissions([os.path.dirname(jaas_path)])
        with open(jaas_path, "w") as f:
            f.write(content)
        setFilePermissions(jaas_path, self.config.get("user", "root"),
                           self.config.get("group", "root"), 0o640)
        return content

    def _on_config_changed(self, event):
        logger.debug("Event triggerd config change: {}".format(event))
        try:
            if self.is_sasl_kerberos_enabled() and not self.keytab:
                self.model.unit.status = \
                    BlockedStatus("Kerberos set, waiting for keytab "
                                  "upload action")
                # We can drop this event given that an action will happen
                # or a config change
                return
        except KafkaCharmBaseMissingConfigError as e:
            # This error is raised if some but not all the configs needed for
            # Kerberos were enabled
            self.model.unit.status = \
                BlockedStatus("Kerberos config missing: {}".format(str(e)))
            return
        if not self._cert_relation_set(event):
            return
#        # handle method must be called before any of the hooks
#        # Running here ensures the handle() is always ran before the relevant
#        # part for the lock management
#        KafkaBrokerCoordinator().handle()
        if not self.zk.relation:
            # It does not make sense to progress until zookeeper is set
            self.model.unit.status = \
                BlockedStatus("Waiting for Zookeeper")
            return
        self.model.unit.status = \
            MaintenanceStatus("Starting to generate certs and keys")
        self._generate_keystores()
        self.model.unit.status = \
            MaintenanceStatus("Render server.properties")

        self.cluster.enable_az = self.config.get(
            "customize-failure-domain", False)

        # START CONFIG FILE UPDATES
        ctx = {}
        ctx = super()._on_config_changed(event)
        try:
            ctx["server_opts"] = self._generate_server_properties(event)
        except KafkaRelationBaseNotUsedError:
            self.model.unit.status = \
                BlockedStatus("Relation not ready yet")
#            event.defer()
            return
        except KafkaListenerRelationEmptyListenerDictError:
            logger.info("Listener info not published, deferring event")
#            event.defer()
            return
        self.model.unit.status = \
            MaintenanceStatus("Render client properties")
        ctx["client_opts"] = self._generate_client_properties()
        self.model.unit.status = \
            MaintenanceStatus("Render service override.conf")
        ctx["svc_opts"] = self.render_service_override_file(
            target="/etc/systemd/system/"
                   "{}.service.d/override.conf".format(self.service))
        ctx["keytab_opts"] = self.keytab_b64

        # Now, restart service
        self.model.unit.status = \
            MaintenanceStatus("Building context...")
        logger.debug("Context: {}, saved state is: {}".format(
            ctx, self.ks.config_state
        ))
        if self._check_if_ready_to_start(ctx):
            self.on.restart_event.emit(ctx, services=[self.service])
            self.model.unit.status = \
                BlockedStatus("Waiting for restart event")
            return
        elif service_running(self.service):
            self.model.unit.status = \
                        ActiveStatus("Service is running")
        else:
            self.model.unit.status = \
                BlockedStatus("Service not running that "
                              "should be: {}".format(self.service))
        # Now, we need to always handle the locks, even if acquire() was not
        # called since _check_if_ready_to_start returned False.
        # Therefore, we need to manually handle those locks.
        # If _check_if_ready_to_start returns True, then the locks will be
        # managed at the restart event and config-changed is closed with a
        # return.
        coordinator = OpsCoordinator()
        coordinator.resume()
        coordinator.release()

        # Apply sysctl

#        if self._check_if_ready_to_start(changed):
#            # Needed to be done on every config-changed
#            KafkaBrokerCoordinator().acquire('restart')
#            if KafkaBrokerCoordinator().granted('restart'):
#                self.model.unit.status = \
#                    MaintenanceStatus("Starting services...")
#                # Unmask and enable service
#                service_resume(self.service)
#                # Reload and restart
#                service_reload(self.service)
#                service_restart(self.service)
#        running = False
#        try:
#            running = service_running(self.service)
#        except: # noqa
#            pass
#        if not running:
#            self.model.unit.status = \
#                BlockedStatus("Service not running {}".format(self.service))
#        if self.config["manual_restart"]:
#            self.model.unit.status = \
#                BlockedStatus("Waiting for manual restart")


if __name__ == "__main__":
    main(KafkaBrokerCharm)
