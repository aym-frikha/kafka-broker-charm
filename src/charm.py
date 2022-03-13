#!/usr/bin/env python3

"""A Juju machine charm for Kafka."""

import logging
import os
import yaml
import json
import hashlib

from ops.main import main
from ops.model import (
    MaintenanceStatus,
    ActiveStatus,
    BlockedStatus
)

from charms.operator_libs_linux.v1.systemd import (
    service_running,
    service_restart,
    service_resume,
    daemon_reload
)

from charms.kafka_broker.v0.kafka_base_class import (
    KafkaJavaCharmBase,
    KafkaCharmBaseMissingConfigError,
    KafkaJavaCharmBasePrometheusMonitorNode,
    KafkaJavaCharmBaseNRPEMonitoring
)
from cluster import KafkaBrokerCluster
from charms.zookeeper.v0.zookeeper import ZookeeperRequiresRelation
from charms.kafka_broker.v0.kafka_relation_base import (
    KafkaRelationBaseNotUsedError,
    KafkaRelationBaseTLSNotSetError
)

import interface_tls_certificates.ca_client as ca_client

from charms.kafka_broker.v0.charmhelper import (
    open_port,
    render
)

from charms.kafka_broker.v0.kafka_security import (
    setFilePermissions,
    genRandomPassword,
    generateSelfSigned,
    PKCS12CreateKeystore,
    CreateTruststore
)
from charms.kafka_broker.v0.kafka_listener import (
    KafkaListenerProvidesRelation,
    KafkaListenerRelationEmptyListenerDictError
)
from charms.kafka_broker.v0.kafka_mds import (
    KafkaMDSProvidesRelation
)
from ops_coordinator.ops_coordinator import (
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
    """Exception raised when MDS relation is but distro is not confluent."""

    def __init__(self, distro):
        """Return error message for mds not supported."""
        message = "MDS is not supported on " + \
                  "{} distribution, only Confluent".format(distro)
        super().__init__(message)


class KafkaBrokerCharm(KafkaJavaCharmBase):
    """Implements the Kafka machine charm."""

    on = RestartCharmEvent()

    def _install_tarball(self):
        """Deploy Kafka from a tarball resource."""
        # Use _generate_service_files here
        raise Exception("Not Implemented Yet")

    def __init__(self, *args):
        """Initialize kafka charm."""
        super().__init__(*args)
        self.certificates = ca_client.CAClient(
            self,
            'certificates')
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
        self.framework.observe(self.certificates.on.ca_available,
                               self.on_certificates_relation_joined)
        self.framework.observe(self.certificates.on.tls_server_config_ready,
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
        # Certificate management methods
        self.framework.observe(self.on.add_certificates_action,
                               self.add_certificates_action)
        self.framework.observe(self.on.remove_certificates_action,
                               self.remove_certificates_action)
        self.framework.observe(self.on.list_certificates_action,
                               self.list_certificates_action)

        self.cluster = KafkaBrokerCluster(self, 'cluster',
                                          self.config.get("cluster-count", 3))
        self.zk = ZookeeperRequiresRelation(self, 'zookeeper')
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
        self.ks.set_default(need_restart=False)
        self.ks.set_default(ports=[])
        self.ks.set_default(endpoints=[])
        # LMA integrations
        self.prometheus = \
            KafkaJavaCharmBasePrometheusMonitorNode(
                self, 'prometheus-manual',
                port=self.config.get("jmx-exporter-port", 9404),
                internal_endpoint=self.config.get(
                    "jmx_exporter_use_internal", False),
                labels=self.config.get("jmx_exporter_labels", None))
        self.nrpe = KafkaJavaCharmBaseNRPEMonitoring(
            self,
            svcs=[],
            endpoints=[],
            nrpe_relation_name='nrpe-external-master')
        # Now, we need to always handle the locks
        # This method is always called, therefore should be used to
        # always manage the locks.
        self.coordinator = OpsCoordinator()
        self.coordinator.resume()
        # List of listeners to be passed via relation if restart is
        # successful
        self.listener_info = None

    def __del__(self):
        """Ensure coordinator will release any locks."""
        self.coordinator.release()

    def is_jmxexporter_enabled(self):
        """Check if prometheus relation exists."""
        if self.prometheus.relations:
            return True
        return False

    def _manage_listener_certs(self):
        """Manages the certificates between cluster and listener relations.

        It is important to remember that cluster relation is actually one listener: BROKER.
        Therefore, any certificates learnt by cluster relations also need to be trusted and
        managed by self.listener.
        """
        # Cluster certificate: this is set using BROKER listener
        if len(self.get_ssl_keystore()) > 0:
            if len(self.get_ssl_cert()) > 0 and \
               len(self.get_ssl_key()) > 0 and \
               len(self.get_ssl_truststore()) > 0:
                logger.info("Setting SSL for client connections")
                user = self.config.get("user", "")
                group = self.config.get("group", "")
                extra_certs = self.cluster.get_all_certs()
                # Grab the extra CAs that have been passed via certificates action
                extra_cas = self.ks.ssl_certs

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
                        extra_certs=extra_certs,
                        extra_cas=extra_cas)
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
                    # self.cluster._get_all_tls_certs()
                    CreateTruststore(self.get_ssl_truststore(),
                                     self.ks.ts_password,
                                     extra_certs,
                                     ts_regenerate=True,
                                     user=self.config["user"],
                                     group=self.config["group"],
                                     mode=0o640,
                                     extra_cas=extra_cas)
                else:
                    logger.info("Neither Listener nor Cluster relations "
                                "create the truststore manually.")
                    CreateTruststore(self.get_ssl_truststore(),
                                     self.ks.ts_password,
                                     [self.get_ssl_cert()],
                                     ts_regenerate=True,
                                     user=self.config["user"],
                                     group=self.config["group"],
                                     mode=0o640,
                                     extra_cas=extra_cas)

    def add_certificates_action(self, event):
        """Loads new CAs into ks.ssl_certs and regenerates truststores."""
        super().add_certificates_action(event.params["cert-files"])
        self._manage_listener_certs()

    def remove_certificates_action(self, event):
        """Removes CAs from ks.ssl_certs and regenerates truststores."""
        super().remove_certificates_action(event.params["cert-files"])
        self._manag_listener_certs()

    def list_certificates_action(self, event):
        return super().list_certificates_action()

    def on_upload_keytab_action(self, event):
        """Implement the keytab action upload."""
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
        """Run the restart logic."""
        if not self.ks.need_restart:
            # There is a chance of several restart events being stacked.
            # This check ensures a single restart happens if several
            # restart events have been requested.
            # In this case, a restart already happened and no other restart
            # has been emitted, therefore, avoid restarting.

            # That is possible because event.restart() acquires the lock,
            # (either at that hook or on a future hook) and then, returns
            # True + release the lock at the end.
            # Only then, we set need_restart to False (no pending lock
            # requests for this unit anymore).
            # We can drop any other restart events that were stacked and
            # waiting for processing.
            if self.listener_info:
                logger.debug("Running bootstrap_data: {}".format(
                    self.listener_info))
                self.listener.set_bootstrap_data(self.listener_info)
            return
        try:
            if event.restart(self.coordinator):
                if self.check_ports_are_open(
                        endpoints=self.ks.endpoints,
                        retrials=3):
                    # Restart was successful, update need_restart and inform
                    # the clients via listener relation
                    self.model.unit.status = \
                        ActiveStatus("service running")
                    # Toggle need_restart as we just did it.
                    self.ks.need_restart = False
                else:
                    logger.warning("Failure at restart, operator should check")
                    self.model.unit.status = \
                        BlockedStatus("Restart Failed, check service")
            else:
                # defer the RestartEvent as it is still waiting for the
                # lock to be released.
                event.defer()
        # Not using SystemdError as it is not exposed
        except Exception as e:
            # except SystemdError:
            logger.warning("Restart failed, blocking unit: {}".format(e))
            self.model.unit.status = \
                BlockedStatus("Restart Failed, check service")
            # Ignore the next restarts
            self.ks.need_restart = False

    def on_certificates_relation_joined(self, event):
        """Request the certificates needed for this unit."""
        # Relation just joined, request certs for each of the relations
        # That will happen once. The certificates will be generated, then
        # it will trigger a -changed Event on certificates, which will
        # call the config-changed logic once again.
        # That way, the certificates will be added to the truststores
        # and shared across the other relations.

        # In case several relations shares the same set of IPs (spaces),
        # the last relation will get to set the certificate values.
        # Therefore, the order of the list below is relevant.
        for r in [self.cluster, self.zk, self.listener]:
            self._cert_relation_set(None, r)
        self._on_config_changed(event)

    def on_certificates_relation_changed(self, event):
        """Check if the certificates are ready and update configs."""
        self._on_config_changed(event)

    def on_listeners_relation_joined(self, event):
        """Execute listener logic."""
        self.listener.on_listener_relation_joined(event)
        self._on_config_changed(event)

    def on_listeners_relation_changed(self, event):
        """Execute listener logic."""
        try:
            self.listener.on_listener_relation_changed(event)
        except KafkaRelationBaseTLSNotSetError:
            logger.warning(
                "Detected some of the remote apps on listener relation "
                "but still waiting for setup on this unit.")
            # We need certs correctly configured to be able to set listeners
            # because certs are configured on other peers.
            # Defer this event until operator updates certificate info.
            self.model.unit.status = BlockedStatus(
                "Missing certificate info: listeners")
            event.defer()
            return
        self._on_config_changed(event)

    def on_mds_relation_joined(self, event):
        """Add the MDS relation for confluent kafka."""
        # TODO: Implement
        return

    def on_mds_relation_changed(self, event):
        """Add the MDS relation for confluent kafka."""
        # TODO: Implement
        return

    def on_update_status(self, event):
        """Update the status of the charm according to service status."""
        # Check if the locks must be handled or not
        # coordinator = OpsCoordinator()
        # coordinator.handle_locks(self.unit)
        super().on_update_status(event)

    def _on_cluster_relation_joined(self, event):
        """Call cluster class for -joined event."""
        self.cluster.user = self.config.get("user", "")
        self.cluster.group = self.config.get("group", "")
        self.cluster.mode = 0o640
        try:
            self.cluster.on_cluster_relation_joined(event)
        except KafkaRelationBaseNotUsedError as e:
            # Relation not been used by any other application, move on
            logger.info(str(e))
        except KafkaRelationBaseTLSNotSetError as e:
            event.defer()
            self.model.unit.status = BlockedStatus(str(e))
        self._on_config_changed(event)

    def _on_cluster_relation_changed(self, event):
        """Call cluster class for -changed event."""
        self.cluster.user = self.config.get("user", "")
        self.cluster.group = self.config.get("group", "")
        self.cluster.mode = 0o640
        try:
            self.cluster.on_cluster_relation_changed(event)
        except KafkaRelationBaseNotUsedError as e:
            # Relation not been used by any other application, move on
            logger.info(str(e))
        except KafkaRelationBaseTLSNotSetError as e:
            event.defer()
            self.model.unit.status = BlockedStatus(str(e))
        self._on_config_changed(event)
        # Inform prometheus there are new units to monitor
        if not self.prometheus.relations:
            return
        if len(self.prometheus.relations) > 0:
            self.prometheus.on_prometheus_relation_changed(event)

    def _on_zookeeper_relation_joined(self, event):
        """Call zk class for -joined event."""
        self.zk.user = self.config.get("user", "")
        self.zk.group = self.config.get("group", "")
        self.zk.mode = 0o640
        try:
            self.zk.on_zookeeper_relation_joined(event)
        except KafkaRelationBaseNotUsedError as e:
            # Relation not been used by any other application, move on
            logger.info(str(e))
        except KafkaRelationBaseTLSNotSetError as e:
            event.defer()
            self.model.unit.status = BlockedStatus(str(e))
        self._on_config_changed(event)

    def _on_zookeeper_relation_changed(self, event):
        """Call zk class for -changed event."""
        self.zk.user = self.config.get("user", "")
        self.zk.group = self.config.get("group", "")
        self.zk.mode = 0o640
        try:
            self.zk.on_zookeeper_relation_changed(event)
        except KafkaRelationBaseNotUsedError as e:
            # Relation not been used by any other application, move on
            logger.info(str(e))
        except KafkaRelationBaseTLSNotSetError as e:
            event.defer()
            self.model.unit.status = BlockedStatus(str(e))
        # A Zookeeper change should always trigger a restart.
        if not service_running(self.service):
            # For some reason, after configurations are ready, kafka restarts
            # before zookeeper is ready. That means the last restart events
            # are lost. Therefore, check here if kafka service is running.
            # If not running before config change, it is worthy to restart it.
            # Not putting this logic into update_status because this is
            # kafka <> zookeeper specific.
            service_resume(self.service)
            service_restart(self.service)
        else:
            # Otherwise, charm is running then issue a restart event.
            self.ks.need_restart = True
            # Issue a restart event with current context.
            self.on.restart_event.emit(
                self.ks.config_state, services=self.services)
        self._on_config_changed(event)

    def _generate_keystores(self):
        """Generate the keystores for SSL and zookeeper relations."""
        # TODO: move to kafka base class

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
        """Run the installation process.

        There are several options for installation, depending on distro.
        """
        self.model.unit.status = MaintenanceStatus("Installing packages...")
        super()._on_install(event)
        packages = []
        if self.config.get("install_method") == "archive":
            # TODO: implement the tarball installation
            self._install_tarball()
        elif self.distro == "confluent" or self.distro == "apache":
            # Both confluent and apache install via packages.
            if self.distro == "confluent":
                packages = CONFLUENT_PACKAGES + ['libsystemd-dev']
            else:
                # TODO: implement apache / package installation
                raise Exception("Not Implemented Yet")
        elif self.distro == "apache_snap":
            # Install via snaps, either the resource attached to the charm
            # or from snapstore.
            # Override prometheus jar file
            self.JMX_EXPORTER_JAR_FOLDER = \
                "/snap/kafka/current/jar/"

        # Install packages will install snap in this case
        super().install_packages('openjdk-11-headless', packages)

        # The logic below avoid an error such as more than one entry
        # In this case, we will pick the first entry
        data_log_fs = \
            list(yaml.safe_load(
                     self.config.get("data-log-dir", "")).items())[0][0]
        data_log_dir = \
            list(yaml.safe_load(
                     self.config.get("data-log-dir", "")).items())[0][1]
        # Create the log folders.
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
        """Check if restart event is necessary.

        If the cluster is not yet ready, return waiting on other units.
        Then, check if the context passed via argument is the same as the last
        context hash seen. If yes, it means there was no relevant configuration
        change that justifies a restart.
        """
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

    def get_ssl_cert(self):
        """Get the certificate from the option or relation."""
        return self._get_ssl_cert(self.listener.binding_addr)

    def get_ssl_key(self):
        """Get the key from the option or relation."""
        return self._get_ssl_key(self.listener.binding_addr)

    def get_ssl_keystore(self):
        """Get the keystore."""
        path = self.config.get("keystore-path", "")
        return path

    def get_ssl_truststore(self):
        """Get the truststore."""
        path = self.config.get("truststore-path", "")
        return path

    def get_zk_keystore(self):
        """Get the keystore."""
        path = self.config.get("keystore-zookeeper-path", "")
        return path

    def get_zk_truststore(self):
        """Get the truststore."""
        path = self.config.get("truststore-zookeeper-path", "")
        return path

    def get_zk_cert(self):
        """Get the certificate from the option or relation."""
        return self._get_ssl_cert(
                   self.zk.binding_addr, "ssl-zk-cert", "ssl-zk-key")

    def get_zk_key(self):
        """Get the key from the option or relation."""
        return self._get_ssl_key(
                   self.zk.binding_addr, "ssl-zk-cert", "ssl-zk-key")

    def get_oauth_token_cert(self):
        """Get OAUTH token certificate."""
        # TODO: implement cert/key generation and
        # management through this getters
        raise Exception("Not Implemented Yet")

    def get_oauth_token_key(self):
        """Get OAUTH token key."""
        # TODO: implement cert/key generation and
        # management through this getters
        raise Exception("Not Implemented Yet")

    def _generate_server_properties(self, event):
        self.model.unit.status = \
            MaintenanceStatus("Starting server.properties")
        server_props = \
            yaml.safe_load(self.config.get("server-properties", "")) or {}
        # https://docs.confluent.io/platform/current/installation/license.html
        if self.get_license_topic() and \
           len(self.get_license_topic()) > 0 and \
           self.distro == "confluent":
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
        # server_props["kafka_broker_rest_proxy_enabled"] = False

        self._manage_listener_certs()

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

            # In case LDAP is configured, MDS endpoints need to be shared.
            # Publish the endpoints to the cluster units:
            cluster_data = self.cluster.relation.data
            cluster_data[self.unit]["mds_url"] = \
                server_props["confluent.metadata.server.advertised.listeners"]
            # Now read each of the units' in the cluster relation
            # Inform listener requirers of MDS endpoint
            self.listener.set_mds_endpoint(
                ",".join(
                    [cluster_data[u]["mds_url"]
                     for u in self.cluster.relation.units]),
                self.config["mds_user"], self.config["mds_password"]
            )
            # Finish MDS configuration
            if len(self.get_ssl_keystore()) > 0:
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

        # Listener logic
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
            self.config["oauth-public-key-path"],
            get_default=True,
            clientauth=self.config.get("clientAuth", False))

        # Open listener ports:
        # for p in self.ks.ports:
        #     close_port(p)
        prts = []
        e_lst = self.listener._convert_listener_template(listeners)
        for k, v in e_lst.items():
            open_port(v["port"])
            prts.append(v["port"])
        self.ks.ports = prts
        # Update endpoints
        endpoints = \
            [v["endpoint"].split("://")[1] for k, v in e_lst.items()]
        self.nrpe.recommit_checks(
            svcs=[],
            endpoints=endpoints
        )
        # This is used in the restart logic
        self.ks.endpoints = endpoints

        if len(self.listener.get_sasl_mechanisms_list()) > 0:
            server_props["sasl.enabled.mechanisms"] = ",".join(
                self.listener.get_sasl_mechanism_list())
            server_props["sasl.kerberos.service.name"] = "HTTP"
            if "protocol" in listeners["broker"]["SASL"]:
                server_props["sasl.mechanism."
                             "inter.broker.protocol"] = \
                    listeners["broker"]["SASL"]

        self.listener_info = listeners
        logger.debug("Found listeners: {}".format(listeners))
        server_props = {**server_props, **listener_opts}

        # Zookeeper options:
        self.model.unit.status = \
            MaintenanceStatus("render_server_properties: Start ZK configs")
        if len(self.get_zk_keystore()) > 0:
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
                    # This may happen if certificates have not yet been set.
                    # Return now. Once certificates relation is updated, a
                    # -changed event will rerun config_changed logic.
                    self.model.unit.status = BlockedStatus(str(e))
                    return

        if len(self.config.get("authorizer-class-name", "")) > 0:
            server_props["authorizer.class.name"] = \
                self.config.get("authorizer-class-name", "")

        server_props["zookeeper.connect"] = self.zk.get_zookeeper_list
        server_props["zookeeper.set.acl"] = self.zk.is_sasl_enabled()

        try:
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
                target = self.config["filepath-zookeeper-client-properties"]
                render(source="zookeeper-tls-client.properties.j2",
                       target=target,
                       owner=self.config.get('user'),
                       group=self.config.get("group"),
                       perms=0o640,
                       context={
                         "client_props": client_props
                       })
                server_props = {**server_props, **client_props}
        except KafkaRelationBaseTLSNotSetError as e:
            # This may happen if certificates have not yet been set.
            # Return now. Once certificates relation is updated, a
            # -changed event will rerun config_changed logic.
            self.model.unit.status = BlockedStatus(str(e))
            return
        logger.debug("Finished server.properties, options: "
                     "{}".format(",".join(server_props)))
        # Back to server.properties, render it
        render(source="server.properties.j2",
               target=self.config["filepath-server-properties"],
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
        try:
            if self.is_ssl_enabled():
                if len(self.get_ssl_truststore()) > 0:
                    client_props["ssl.truststore.location"] = \
                        self.get_ssl_truststore()
                    client_props["ssl.truststore.password"] = \
                        self.ks.ts_password
                else:
                    logger.debug("truststore-path not set, "
                                 "using Java own truststore instead")
            logger.debug("Finished client.properties, options are: "
                         "{}".format(",".join(client_props)))
        except KafkaRelationBaseTLSNotSetError as e:
            # This may happen if certificates have not yet been set.
            # Return now. Once certificates relation is updated, a
            # -changed event will rerun config_changed logic.
            self.model.unit.status = BlockedStatus(str(e))
            return
        render(source="client.properties.j2",
               target=self.config["filepath-kafka-client-properties"],
               owner=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o640,
               context={
                   "client_props": client_props
               })
        return client_props

    def _get_service_name(self):
        """Return the service name based on the distro selected."""
        if self.distro == "confluent":
            self.service = "confluent-server"
        elif self.distro == "apache_snap":
            self.service = "snap.kafka.kafka"
        else:
            self.service = "kafka"
        return self.service

    def _render_jaas_conf(self, jaas_path="/etc/kafka/jaas.conf"):
        """Get the JAAS config for authentication."""
        content = ""
        jaas_file = self.config["filepath-jaas-conf"]
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
        self.set_folders_and_permissions([os.path.dirname(jaas_file)])
        with open(jaas_file, "w") as f:
            f.write(content)
        setFilePermissions(jaas_file, self.config.get("user", "root"),
                           self.config.get("group", "root"), 0o640)
        return content

    def _render_kafka_log4j_properties(self):
        root_logger = self.config.get("log4j-root-logger", None) or \
            "INFO, stdout, file"
        if self.distro == "apache_snap":
            kafka_logger_path = "/var/snap/kafka/common/kafka.log"
        else:
            kafka_logger_path = "/var/log/kafka/kafka.log"
        self.model.unit.status = MaintenanceStatus("Rendering log4j...")
        logger.debug("Rendering log4j")
        target = self.config["filepath-log4j-properties"]
        render(source="log4j.properties.j2",
               target=target,
               owner=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o640,
               context={
                   "log4j_root_logger": root_logger,
                   "kafka_logger_path": kafka_logger_path,
               })
        return root_logger

    def _on_config_changed(self, event):
        """Do the configuration change.

        1) Check if kerberos and ZK is available
        2) Generate the Keystores
        3) Manage AZs
        4) Generate the config files
        5) Restart strategy
        6) Open ports
        """
        logger.debug("EVENT DEBUG: _on_config_changed called"
                     " for event: {}".format(event))
        # START CONFIG FILE UPDATES
        ctx = {}

        logger.debug("Event triggered config change: {}".format(event))
        # 1) Check Kerberos and ZK
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
        parent_config = super()._on_config_changed(event)
        if not self.zk.relation:
            # It does not make sense to progress until zookeeper is set
            self.model.unit.status = \
                BlockedStatus("Waiting for Zookeeper")
            return
        # 2) Generate Keystores
        self.model.unit.status = \
            MaintenanceStatus("Starting to generate certs and keys")
        self._generate_keystores()
        self.model.unit.status = \
            MaintenanceStatus("Render server.properties")
        # 3) Manage AZs
        self.cluster.enable_az = self.config.get(
            "customize-failure-domain", False)

        # 4) Generate the config files
        try:
            server_opts = self._generate_server_properties(event)
        except KafkaRelationBaseNotUsedError:
            self.model.unit.status = \
                BlockedStatus("Relation not ready yet")
            return
        except KafkaListenerRelationEmptyListenerDictError:
            logger.info("Listener info not published, deferring event")
            return
        self.model.unit.status = \
            MaintenanceStatus("Render client properties")
        client_opts = self._generate_client_properties()
        self.model.unit.status = \
            MaintenanceStatus("Render service override.conf")
        if self.distro == "apache_snap":
            svc_opts = self.render_service_override_file(
                target="/etc/systemd/system/"
                       "{}.service.d/override.conf".format(self.service),
                jmx_jar_folder="/snap/kafka/current/jar/",
                jmx_file_name="/var/snap/kafka/common/prometheus.yaml")
        else:
            svc_opts = self.render_service_override_file(
                target="/etc/systemd/system/"
                       "{}.service.d/override.conf".format(self.service))
        # Reload service
        daemon_reload()

        log4j_opts = self._render_kafka_log4j_properties()

        ctx = hashlib.md5(json.dumps({
            "init_config": parent_config,
            "server_opts": server_opts,
            "log4j_opts": log4j_opts,
            "svc_opts": svc_opts,
            "client_opts": client_opts,
            "keytab_opts": self.keytab_b64,
            "certificates": {
                "ssl_crt": self.get_ssl_cert(),
                "ssl_key": self.get_ssl_key(),
                "ssl_ks": self.get_ssl_keystore(),
                "ssl_ts": self.get_ssl_truststore(),
                "zk_crt": self.get_zk_cert(),
                "zk_key": self.get_zk_key(),
                "zk_ks": self.get_zk_keystore(),
                "zk_ts": self.get_zk_truststore(),
            }
        }).encode('utf-8')).hexdigest()

        # 5) Restart Strategy
        # Now, service is operational. Restart service with an event to
        # avoid any conflicts with other running units.
        self.model.unit.status = \
            MaintenanceStatus("Building context...")
        logger.debug("Context: {}, saved state is: {}".format(
            ctx, self.ks.config_state))

        if self._check_if_ready_to_start(ctx):
            self.on.restart_event.emit(ctx, services=self.services)
            self.ks.need_restart = True
            self.model.unit.status = \
                BlockedStatus("Waiting for restart event")
        elif service_running(self.service):
            self.model.unit.status = \
                ActiveStatus("Service is running")
        else:
            self.model.unit.status = \
                BlockedStatus("Service not running that "
                              "should be: {}".format(self.services))
        self.ks.config_state = ctx


if __name__ == "__main__":
    main(KafkaBrokerCharm)
