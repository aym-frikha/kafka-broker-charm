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

"""

Implements the common code across the Kafka stack charms.

There are several common tasks that are contemplated here:
    * install packages: install a list of packages + OpenJDK
    * Authentication: there are several mechanisms and they repeat themselves
    * Render config files such as override.conf for services
    * Manage certificates relations or options

Kafka can come from several distributions and versions. That can be selected
using config options: "distro" and "version". Charms that inherit from this
class should implement these configs.

The Distro allows to select one between 3 options of Kafka packages:

- Confluent:   uses confluent repos to install packages installs the confluent
               stack, including confluent center. Please, check your permissio
               to these packages and appropriate license.
- Apache:      Uses Canonical ESM packages (EXPERIMENTAL)
- Apache Snap: Uses snaps available in snapstore or uploaded as resources.

To use it, Kafka charms must call the equivalent events such as:

class KafkaCharmFinal(KafkaJavaCharmBase):

    def __init__(self, *args):
        ...

    def _on_install(self, event):
        super()._on_install(event)

    def _on_config_changed(self, event):
        super()._on_config_changed(event)

    def _on_update_status(self, event):
        super().on_update_status(event)



Besides the shared event handling as shown above, kafka.py also contains
classes that can be used to setup the LMA integration. For that, the Kafka
final charm should use:

    KafkaJavaCharmBaseNRPEMonitoring
    KafkaJavaCharmBasePrometheusMonitorNode

"""

import time
import base64
import os
import shutil
import subprocess
import logging
import yaml
import socket

import pwd
import grp

import cryptography.hazmat.primitives.serialization as serialization

from charms.kafka_broker.v0.java_class import JavaCharmBase
from charms.kafka_broker.v0.kafka_linux import (
    userAdd,
    groupAdd,
    LinuxUserAlreadyExistsError,
    LinuxGroupAlreadyExistsError
)

from charms.operator_libs_linux.v1.systemd import (
    service_running,
)

from ops.framework import (
    Object,
)

from ops.model import (
    BlockedStatus,
    MaintenanceStatus,
    ActiveStatus
)

from charms.kafka_broker.v0.charmhelper import (
    apt_update,
    add_source,
    mount,
    render,
    render_from_string
)

from charms.kafka_broker.v0.kafka_security import setFilePermissions

import interface_tls_certificates.ca_client as ca_client

from charms.kafka_broker.v0.charmhelper import (
    get_hostname
)
from charms.kafka_broker.v0.kafka_prometheus_monitoring import (
    BasePrometheusMonitor,
    BasePrometheusMonitorMissingEndpointInfoError
)

from nrpe.client import NRPEClient
from charms.kafka_broker.v0.charmhelper import (
    open_port
)

from charms.kafka_broker.v0.kafka_storage_manager import StorageManager, StorageManagerError

logger = logging.getLogger(__name__)

__all__ = [
    'KafkaJavaCharmBase',
    'KafkaJavaCharmBaseNRPEMonitoring',
    'KafkaJavaCharmBasePrometheusMonitorNode',
    'KafkaCharmBaseConfigNotAcceptedError',
    'KafkaCharmBaseMissingConfigError',
    'KafkaCharmBaseFeatureNotImplementedError',
    'OVERRIDE_CONF',
    'KRB5_CONF'
]


# To avoid having to add this template to each charm, generate from a
# string instead.
OVERRIDE_CONF = """{% if service_unit_overrides %}
[Unit]
{% for key, value in service_unit_overrides.items() %}
{% if value %}
{{key}}={{value}}
{% endif %}
{% endfor %}

{% endif %}
[Service]
{% for key, value in service_overrides.items() %}
{% if value %}
{% if key =='ExecStart' %}
# If there is an ExecStart override then we need to clear the ExecStart list first
ExecStart=
{% endif %}
{{key}}={{value}}
{% endif %}
{% endfor %}
{% for key, value in service_environment_overrides.items() %}
{% if value %}
Environment="{{key}}={{value}}"
{% endif %}
{% endfor %}""" # noqa

KRB5_CONF = """[libdefaults]
 default_realm = {{ realm|upper() }}
 dns_lookup_realm = {{ dns_lookup_realm }}
 dns_lookup_kdc = {{ dns_lookup_kdc }}
 ticket_lifetime = {{ ticket_lifetime }}
 forwardable = {{ forwardable }}
 udp_preference_limit = {{ udp_preference_limit }}
 default_tkt_enctypes = {{ default_tkt_enctypes }}
 default_tgs_enctypes = {{ default_tgs_enctypes }}
 permitted_enctypes = {{ permitted_enctypes }}

[realms]
 {{ realm| upper() }} = {
  kdc = {{ kdc_hostname }}:88
  admin_server = {{ admin_hostname }}:749
  default_domain = {{ realm|lower() }}
 }

[domain_realm]
 .{{ realm|lower() }} = {{ realm|upper() }}
  {{ realm|lower() }} = {{ realm|upper() }}""" # noqa


def calculate_resource_checksum(resource):
    """Calculates a checksum for a resource"""
    import hashlib

    md5 = hashlib.md5()
    path = hookenv.resource_get(resource)
    if path:
        with open(path, "rb") as f:
            data = f.read()
        md5.update(data)
    return md5.hexdigest()


class KafkaJavaCharmBasePrometheusMonitorNode(BasePrometheusMonitor):
    """Prometheus Monitor node issues a request for prometheus2 to
    setup a scrape job against its address.

    Space of the relation will be used to generate address.


    Implement the following methods in the your KafkaJavaCharmBase.

    # Override the method below so the KafkaJavaCharmBase can know if
    # prometheus relation is being used.
    def is_jmxexporter_enabled(self):
        if self.prometheus.relations:
            return True
        return False

    ...

    def __init__(self, *args):

        self.prometheus = \
            KafkaJavaCharmBasePrometheusMonitorNode(
                self, 'prometheus-manual',
                port=self.config.get("jmx-exporter-port", 9404),
                internal_endpoint=self.config.get(
                    "jmx_exporter_use_internal", False),
                labels=self.config.get("jmx_exporter_labels", None))

        # Optionally, listen to prometheus_job_available events
        self.framework.observe(
            self.prometheus.on.prometheus_job_available,
            self.on_prometheus_job_available)
    """

    def __init__(self, charm, relation_name,
                 port=9404, internal_endpoint=False, labels=None):
        """Args:
            charm: CharmBase object
            relation_name: relation name with prometheus
            port: port value to open
            internal_endpoint: defines if the relation
            labels: str comma-separated key=value labels for the job.
                Converted to dict and passed in the request
        """
        super().__init__(charm, relation_name)
        self.port = port
        self.endpoint = \
            self.binding_addr if internal_endpoint else self.advertise_addr
        self.labels = None
        if labels:
            self.labels = {
                la.split("=")[0]: la.split("=")[1] for la in labels.split(",")
            }
        open_port(self.port)
        self.framework.observe(
            self.on.prometheus_job_available,
            self.on_prometheus_job_available)

    def on_prometheus_job_available(self, event):
        try:
            self.scrape_request_all_peers(
                port=self.port,
                metrics_path="/",
                labels=self.labels)
        except BasePrometheusMonitorMissingEndpointInfoError:
            # This is possible to happen if the following sequence
            # of events happens:
            # 1) cluster-changed: new peer updated and added endpoint
            # 2) prometheus-changed event: peer info recovered
            #       issue the -available event
            # 3) cluster-joined: new peer in the relation
            # 4) prometheus-available:
            #       there was no time for the new peer to add its own
            #       endpoint information. Reinvoke prometheus-changed
            #       on the worst case, this event will be deferred
            self.on_prometheus_relation_changed(event)


class KafkaJavaCharmBaseNRPEMonitoring(Object):
    """Kafka charm used to add NRPE support.

    Create an object of this class to hold the NRPE relation info.


    To instantiate this NRPE, use the following logic:

    def __init__(self, *args):
        self.nrpe = KafkaJavaCharmBaseNRPEMonitoring(
            self,
            svcs=["kafka"],
            endpoints=["127.0.0.1:9000"],
            nrpe_relation_name='nrpe-external-master')


    The class inherits from Object instead of a relation so it can capture
    events for NRPE relation.
    """

    def __init__(self, charm, svcs=[], endpoints=[],
                 nrpe_relation_name='nrpe-external-master'):
        """Initialize with the list of services and ports to be monitored.

        Args:
            svcs: list -> list of service names to be monitored by NRPE
                on systemd.
            endpoints: list -> list of endpoints (IP/hostname:PORT)
                to be monitored by NRPE using check_tcp.
            nrpe_relation_name: str -> name of the relation
        """

        super().__init__(charm, nrpe_relation_name)
        self.nrpe = NRPEClient(charm, nrpe_relation_name)
        self.framework.observe(self.nrpe.on.nrpe_available,
                               self.on_nrpe_available)
        self.services = svcs
        self.endpoints = endpoints
        # uses the StoredState from the parent class
        self.nrpe.state.set_default(is_available=False)

    def recommit_checks(self, svcs, endpoints):
        """Once the relation is built and nrpe-available was already
        executed. Run this method to: (1) clean existing checks; and
        (2) submit new ones. That allows update checks following events
        such as config changes.
        """
        # Update internal values:
        self.svcs = svcs
        self.endpoints = endpoints
        if not self.nrpe.state.is_available:
            # on_nrpe_available not yet called, just update internal
            # values and return
            return
        # Reset checks
        self.nrpe.state.checks = {}
        # Rerun the logic to add checks
        self.on_nrpe_available(None)

    def on_nrpe_available(self, event):
        # Deal with services list first:
        for s in self.services:
            check_name = "check_{}_{}".format(
                self.model.unit.name.replace("/", "_"), s)
            self.nrpe.add_check(command=[
                '/usr/local/lib/nagios/plugins/check_systemd.py', s
            ], name=check_name)
        # Deal with endpoints: need to separate the endpoints
        for e in self.endpoints:
            if ":" not in e:
                # No port specified, jump this endpoint
                continue
            hostname = e.split(":")[0]
            port = e.split(":")[1]
            check_name = "check_{}_port_{}".format(
                self.model.unit.name.replace("/", "_"), port)
            self.nrpe.add_check(command=[
                '/usr/lib/nagios/plugins/check_tcp',
                '-H', hostname,
                '-p', port,
            ], name=check_name)
        # Save all new checks to filesystem and to Nagios
        self.nrpe.commit()
        self.nrpe.state.is_available = True


class KafkaCharmBaseMissingRelationError(Exception):

    def __init__(self, relation):
        super().__init__("Missing relation {}".format(relation))
        self._relation = relation

    @property
    def relation(self):
        return self._relation


class KafkaCharmBaseConfigNotAcceptedError(Exception):

    def __init__(self, message):
        super().__init__(message)


class KafkaCharmBaseMissingConfigError(Exception):

    def __init__(self,
                 config_name):
        super().__init__("Missing config: {}".format(config_name))


class KafkaCharmBaseFeatureNotImplementedError(Exception):

    def __init__(self,
                 message="This feature has not"
                         " been implemented yet"):
        super().__init__(message)


class KafkaCharmBaseFailedInstallation(Exception):

    def __init__(self,
                 message="An error occurred during installation: {}",
                 error=""):
        super().__init__(message.format(error))


class KafkaJavaCharmBase(JavaCharmBase):

    LATEST_VERSION_CONFLUENT = "6.1"
    JMX_EXPORTER_JAR_NAME = "jmx_prometheus_javaagent.jar"

    @property
    def unit_folder(self):
        """Returns the unit's charm folder."""

        # Using as a method so we can also mock it on unit tests
        return os.getenv("JUJU_CHARM_DIR")

    @property
    def distro(self):
        """Returns the distro option selected. Values accepted are:

        - Confluent: will use packages coming from confluent repos
        - Apache: uses Canonical's ESM version of kafka
        - Apache Snap: uses snaps instead of packages.

        Children classes should inherit and override this method with
        their own method to load each of the three types above.
        """
        return self.config.get("distro", "confluent").lower()

    def restart(self):
        """Restarts the services. Should be defined by the final classes"""
        pass

    def do_upgrade(self, event):
        """Runs the upgrade action by rerunning installation"""
        self._on_install(event)
        self._on_config_changed(event)
        self.restart()

    def _get_service_name(self):
        """To be overloaded: returns the name of the Kafka service this
        charm must look after.
        """
        return None

    def __init__(self, *args):
        super().__init__(*args)
        self.service = self._get_service_name()
        # This folder needs to be set as root
        os.makedirs("/var/ssl/private", exist_ok=True)
        # Variable to be used to hold TLSCertificatesRelation object
        self.certificates = None
        # List of callable methods that allow to get all the SSL certs/keys
        # This list will be used to iterate over each of the methods on
        # is_ssl_enabled
        self.get_ssl_methods_list = []
        self._kerberos_principal = None
        self.ks.set_default(keytab="")
        self.ks.set_default(ssl_certs=[])
        self._sasl_protocol = None
        # Save the internal content of keytab file from the action.
        # use it as part of the context in the config_changed
        self.keytab_b64 = ""
        self.services = [self.service]
        self.JMX_EXPORTER_JAR_FOLDER = "/opt/prometheus/"
        # Initiates the StorageManager
        self.sm = StorageManager(self)
        # Set the user and group if available as configs
        if "user" in self.config:
            self.sm.set_default_user(self.config["user"], mandatory=True)
        if "group" in self.config:
            self.sm.set_default_group(self.config["group"], mandatory=True)

    def manage_volumes(self):
        """Mounts / umounts volumes according to config option: log-dir.

        If log-dir is not present in self.config, log a warning and return.
        """        
        if "log-dir" not in self.config:
            logger.warn(
                "log-dir config not found, manage_volumes returning,"
                " nothing done")
            return
        logdir = self.config.get("log-dir", [])
        if isinstance(logdir, str):
            logdir = yaml.safe_load(self.config.get("log-dir", '[]'))
        if isinstance(logdir, dict):
            # If this is just
            logdir = [logdir]
        try:
            self.sm.manage_volumes(logdir)
        except StorageManagerError as e:
            # Error happened when mounting a volume, mount it as a charm:
            raise KafkaCharmBaseConfigNotAcceptedError(e.msg)

    def on_update_status(self, event):
        """ This method will update the status of the charm according
            to the app's status

        If the unit is stuck in Maintenance or Blocked, keep the status
        as it is up to the operator to fix these.

        If a child class will implement update-status logic, it must
        call this method as the last task, so the status is correctly set.
        """

        if isinstance(self.model.unit.status, MaintenanceStatus):
            # Log the fact the unit is already blocked and return
            logger.warn(
                "update-status called but unit is in maintenance "
                "status, with message {}, return".format(
                    self.model.unit.status.message))
            return
        if isinstance(self.model.unit.status, BlockedStatus):
            logger.warn(
                "update-status called but unit is blocked, with "
                "message: {}, return".format(
                    self.model.unit.status.message
                )
            )

        # Unit is neither Blocked, nor in Maintenance, start the checks.
        if not self.services:
            self.services = [self.service]

        svc_list = [s for s in self.services if not service_running(s)]
        if len(svc_list) == 0:
            self.model.unit.status = \
                ActiveStatus("{} is running".format(self.service))
            # The status is not in Maintenance and we can see the service
            # is up, therefore we can switch to Active.
            return
        if isinstance(self.model.unit.status, BlockedStatus):
            # Log the fact the unit is already blocked and return
            logger.warn(
                "update-status called but unit is in blocked "
                "status, with message {}, return".format(
                    self.model.unit.status.message))
            return
        self.model.unit.status = \
            BlockedStatus("Services not running that"
                          " should be: {}".format(",".join(svc_list)))

    @property
    def kerberos_principal(self):
        # TODO(pguimaraes): check if domain variable below has len > 0
        # If not, get the IP of the default gateway and rosolve its fqdn
        hostname = "{}.{}".format(
            socket.gethostname(),
            self.config.get("kerberos-domain", ""))

        self._kerberos_principal = "{}/{}@{}".format(
            self.config.get("kerberos-protocol", "").upper(),
            hostname,
            self.config.get("kerberos-realm", "").upper())
        return self._kerberos_principal

    @kerberos_principal.setter
    def kerberos_principal(self, k):
        self._kerberos_principal = k

    @property
    def keytab(self):
        return self.ks.keytab

    @keytab.setter
    def keytab(self, k):
        self.ks.keytab = k

    @property
    def sasl_protocol(self):
        return self._sasl_protocol

    @sasl_protocol.setter
    def sasl_protocol(self, s):
        self._sasl_protocol = s

    def _recover_certificates_from_file(self, cert_files):
        """Implements a helper method for the certificate actions.

        Recovers all the certs from cert_files."""
        certs = []
        for crt in cert_files if isinstance(cert_files,list) else [cert_files]:
            with open(crt) as f:
                certs.append("".join(f.readlines()))
        return certs

    def add_certificates_action(self, cert_files):
        """Adds certificates to self.ks.ssl_certs. This list should be
        used to add custom certificates the entire stack needs to trust.

        Args:
        - certs_files: list of certificate files to add.
        """
        certs = self._recover_certificates_from_file(cert_files)
        if len(certs) == 0:
            # No new cert to add
            return
        to_add = [c for c in certs if c not in self.ks.ssl_certs]
        self.ks.ssl_certs.extend(to_add)
        return self.ks.ssl_certs

    def list_certificates_action(self):
        """Returns the list of certs added to the keystore."""
        return self.ks.ssl_certs

    def remove_certificates_action(self, cert_files):
        """Empties out all the certs passed via action"""
        certs = self._recover_certificates_from_file(cert_files)
        if len(certs) == 0:
            # No new cert to add
            return
        new_ssl_certs = [c for c in self.ks.ssl_certs if c not in certs]
        self.ks.ssl_certs = new_ssl_certs

    def _upload_keytab_base64(self, k, filename="kafka.keytab"):
        """Receives the keytab in base64 format and saves to correct file"""

        filepath = "/etc/security/keytabs/{}".format(filename)
        self.set_folders_and_permissions(
            [os.path.dirname(filepath)],
            mode=0o755)
        with open(filepath, "wb") as f:
            f.write(base64.b64decode(k))
            f.close()
        self.keytab_b64 = str(k)
        setFilePermissions(filepath, self.config.get("user", "root"),
                           self.config.get("group", "root"), 0o640)
        self.keytab = filename

    def check_ports_are_open(self, endpoints,
                             retrials=3, backoff=60):
        """Check if a list of ports is open. Should be used with RestartEvent
        processing. That way, one can separate the restart worked or not.

        The idea is to iterate over the port list and check if they are all
        open. If yes, return True. If not, wait for "backoff" seconds and
        retry. If reached "retrials" and still not all the ports are open,
        return False.

        Args:
        - endpoints: list of strings - endpoints in format hostname/IP:PORT
        - retrials: Number of retries that will be executed
        - backoff: Amount of time to wait after a failed trial, in seconds
        """
        for c in range(retrials):
            success = True
            for ep in endpoints:
                sock = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                port = int(ep.split(":")[1])
                ip = ep.split(":")[0]
                if sock.connect_ex((ip, port)) != 0:
                    success = False
                    break
            if success:
                # We were successful in checking each endpoint
                return True
            # Otherwise, we've failed, sleep for some time and retry
            time.sleep(backoff)
        return False

    @property
    def snap(self):
        """Returns the snap name if apache_snap is specified.

        Must be overloaded by each child class to define its own snap name.
        """
        return "kafka"

    def install_packages(self, java_version, packages, snap_connect=None, masked_services=None):
        """Install the packages/snaps related to the chosen distro.

        Args:
        java_version: install the openjdk corresponding to that version
        packages: package list to be installed
        snap_connect: interfaces that need to be explicitly connected if snap
                      option is chosen instead.
        masked_services: list of service names to be masked before installing the packages.
                         This is useful for snaps, where several services are installed at once
        """

        for s in (masked_services or []):
            subprocess.check_output(["sudo", "systemctl", "--global", "mask", s])

        MaintenanceStatus("Installing packages")
        version = self.config.get("version", self.LATEST_VERSION_CONFLUENT)
        if self.distro == "confluent":
            url_key = 'https://packages.confluent.io/deb/{}/archive.key'
            key = subprocess.check_output(
                      ['wget', '-qO', '-',
                       url_key.format(version)]).decode("ascii")
            url_apt = \
                'deb [arch=amd64] https://packages.confluent.io/deb/{}' + \
                ' stable main'
            add_source(
                url_apt.format(version),
                key=key)
            apt_update()
        elif self.distro == "apache":
            raise Exception("Not Implemented Yet")
        elif self.distro == "apache_snap":
            # First, try to fetch the snap resource:
            resource = None
            try:
                # Fetch a resource with the same name as the snap.
                # Fetch raises a ModelError if not found
                resource = self.model.resources.fetch(self.snap)
                subprocess.check_output(
                    ["snap", "install", "--dangerous", str(resource)])
            except subprocess.CalledProcessError as e:
                raise KafkaCharmBaseFailedInstallation(
                    error=str(e))
            except Exception:
                # Failed to find a resource, next step is to try install
                # from snapstore
                resource = None
            if not resource:
                # Resource was not found, try install from upstream
                try:
                    subprocess.check_output(
                        ["snap", "install", self.snap,
                         "--channel={}".format(version)])
                except subprocess.CalledProcessError as e:
                    raise KafkaCharmBaseFailedInstallation(
                        error=str(e))
            try:
                if snap_connect:
                    for conn in snap_connect:
                        subprocess.check_output(
                            ["snap", "connect",
                             "{}:{}".format(self.snap, conn)])
            except subprocess.CalledProcessError as e:
                raise KafkaCharmBaseFailedInstallation(
                    error=str(e))
            # Install openjdk for keytool
            super().install_packages(java_version, packages=[])
            # Packages will be already in place for snap
            # no need to run the logic below and setup folders
            return

        # Now, only package type distros are available
        # Setup folders, permissions, etc
        super().install_packages(java_version, packages)
        folders = [
            "/etc/kafka",
            "/var/log/kafka",
            "/var/lib/kafka",
            self.JMX_EXPORTER_JAR_FOLDER]
        self.set_folders_and_permissions(folders)

        # Now, setup jmx exporter logic
        self.jmx_version = self.config.get("jmx_exporter_version", "0.12.0")
        jar_url = self.config.get(
            "jmx_exporter_url",
            "https://repo1.maven.org/maven2/io/prometheus/jmx/"
            "jmx_prometheus_javaagent/{}/"
            "jmx_prometheus_javaagent-{}.jar")
        self.jmx_jar_url = jar_url.format(
                self.jmx_version, self.jmx_version)
        if len(self.jmx_version) == 0 or len(self.jmx_jar_url) == 0:
            # Not enabled, finish the method
            return
        subprocess.check_output(
            ['wget', '-qO',
             self.JMX_EXPORTER_JAR_FOLDER + self.JMX_EXPORTER_JAR_NAME,
             self.jmx_jar_url])
        setFilePermissions(
            self.JMX_EXPORTER_JAR_FOLDER + self.JMX_EXPORTER_JAR_NAME,
            self.config.get("user", "kafka"),
            self.config.get("group", "kafka"), 0o640)

    def _get_api_url(self, advertise_addr):
        """Returns the API endpoint for a given service. If the config
        is not manually set, then the advertise_addr is used instead."""

        return "{}://{}:{}".format(
            "https" if self.is_ssl_enabled() else "http",
            self.config["api_url"] if len(self.config["api_url"]) > 0
            else get_hostname(advertise_addr),
            self.config.get("clientPort", 0)
        )

    def set_folders_and_permissions(self, folders, mode=0o750):
        # Check folder permissions
        MaintenanceStatus("Setting up permissions")
        uid = pwd.getpwnam(self.config.get("user", "root")).pw_uid
        gid = grp.getgrnam(self.config.get("group", "root")).gr_gid
        for f in folders:
            os.makedirs(f, mode=mode, exist_ok=True)
            os.chown(f, uid, gid)

    def is_ssl_enabled(self):
        # We will OR rel_set with each available method for getting cert/key
        # Should start with False
        rel_set = True
        for m in self.get_ssl_methods_list:
            rel_set = rel_set and m()
        if not rel_set:
            logger.warning("Only some of the ssl configurations have been set")
        return rel_set

    def is_rbac_enabled(self):
        if self.distro == "apache":
            return False
        return False

    def _get_ldap_settings(self, mds_urls):
        result = "org.apache.kafka.common.security.oauthbearer." + \
            "OAuthBearerLoginModule required " + \
            'username="{}" password="{}" ' + \
            'metadataServerUrls="{}";'
        return result.format(
            self.config["mds_user"], self.config["mds_password"],
            mds_urls)

    def _get_ssl_cert(self, cn=None, crt_config="ssl_cert", key_config="ssl_key"):
        """Recovers the TLS certificate based either if the cert has been passed
        as a configuration parameter (based on crt_config and key_config names)
        or via relation on certificates.

        Args:
            cn: str, common name
            crt_config: str, option name for the base64 config of the certificate
            key_config: str, option name for the base64 config of the key
        """
        if cn==None:
            return ""
        if self.config["generate-root-ca"]:
            return self.ks.ssl_cert
        if len(self.config.get(crt_config)) > 0 and \
           len(self.config.get(key_config)) > 0:
            return base64.b64decode(self.config[crt_config]).decode("ascii")
        # Not a config option, check the certificates relation
        root_ca_chain = None
        ca_cert = None
        try:
            root_ca_chain = self.certificates.root_ca_chain.public_bytes(
                encoding=serialization.Encoding.PEM
            )
        except ca_client.CAClientError:
            # A root ca chain is not always available. If configured to just
            # use vault with self-signed certificates, you will not get a ca
            # chain. Instead, you will get a CAClientError being raised. For
            # now, use a bytes() object for the root_ca_chain as it shouldn't
            # cause problems and if a ca_cert_chain comes later, then it will
            # get updated.
            root_ca_chain = bytes()
        try:
            ca_cert = (
                self.certificates.ca_certificate.public_bytes(
                    encoding=serialization.Encoding.PEM) +
                root_ca_chain)
        except ca_client.CAClientError:
            ca_cert = bytes()
        try:
            certs = self.certificates._get_certs_and_keys(request_type='server')
            c = certs[cn]["cert"].public_bytes(
                    encoding=serialization.Encoding.PEM
                ) + ca_cert
            logger.debug("SSL Certificate chain"
                         " from tls-certificates: {}".format(c))
        except (ca_client.CAClientError):
            # Certificates not ready yet, return empty
            return ""
        return c.decode("utf-8")

    def _get_ssl_key(self, cn=None, crt_config="ssl_cert", key_config="ssl_key"):
        """Recovers the TLS key based either if the cert has been passed
        as a configuration parameter (based on crt_config and key_config names)
        or via relation on certificates.

        Args:
            cn: str, common name
            crt_config: str, option name for the base64 config of the certificate
            key_config: str, option name for the base64 config of the key
        """
        if cn==None:
            return ""
        if self.config["generate-root-ca"]:
            return self.ks.ssl_key
        if len(self.config.get(crt_config)) > 0 and \
           len(self.config.get(key_config)) > 0:
            return base64.b64decode(self.config[key_config]).decode("ascii")
        try:
            certs = self.certificates._get_certs_and_keys(request_type='server')
            k = certs[cn]["key"].private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption())
        except (ca_client.CAClientError):
            # Certificates not ready yet, return empty
            return ""
        return k.decode("utf-8")

    def _cert_relation_set(self, event, rel=None, extra_sans=[]):
        # Will introduce this CN format later
        def __get_cn():
            return "*." + ".".join(socket.getfqdn().split(".")[1:])
        # generate cert request if tls-certificates available
        # rel may be set to None in cases such as config-changed
        # or install events. In these cases, the goal is to run
        # the validation at the end of this method
        if rel:
            if self.certificates.is_joined:
                sans = [
                    socket.gethostname(),
                    socket.getfqdn()
                ]
                # We do not need to know if any relations exists but rather
                # if binding/advertise addresses exists.
                if rel.binding_addr:
                    sans.append(rel.binding_addr)
                if rel.advertise_addr:
                    sans.append(rel.advertise_addr)
                if rel.hostname:
                    sans.append(rel.hostname)
                sans += extra_sans

                # Common name is always CN as this is the element
                # that organizes the cert order from tls-certificates
                self.certificates.request_server_certificate(
                    common_name=rel.hostname,
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
            if (not self.get_ssl_cert() or not self.get_ssl_key()):
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
        except (ca_client.CAClientError,
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

    def is_sasl_enabled(self):
        s = self.config.get("sasl-protocol", None)
        if not s:
            return False
        self.sasl_protocol = s.lower()
        s = self.sasl_protocol
        if s == "oauthbearer":
            return self.is_sasl_oauthbearer_enabled()
        if s == "ldap":
            return self.is_sasl_ldap_enabled()
        if s == "scram":
            return self.is_sasl_scram_enabled()
        if s == "plain":
            return self.is_sasl_plain_enabled()
        if s == "delegate-token":
            return self.is_sasl_delegate_token_enabled()
        if s == "kerberos":
            return self.is_sasl_kerberos_enabled()
        if s == "digest":
            return self.is_sasl_digest_enable()
        raise KafkaCharmBaseConfigNotAcceptedError(
            "None of the options for sasl-protocol are accepted. "
            "Please provide one of the following: oauthbearer, scram,"
            "plain, delegate-token, kerberos, digest.")

    def is_sasl_oauthbearer_enabled(self):
        return False

    def is_sasl_scram_enabled(self):
        return False

    def is_sasl_plain_enabled(self):
        return False

    def is_sasl_delegate_token_enabled(self):
        return False

    def is_sasl_ldap_enabled(self):
        if len(self.config.get("mds_user", "")) > 0 and \
           len(self.config.get("mds_password", "")) > 0 and \
           self.distro == "confluent":
            return True
        return False

    def is_sasl_kerberos_enabled(self):
        mandatory_options = [
            "kerberos-protocol",
            "kerberos-realm",
            "kerberos-domain",
            "kerberos-kdc-hostname",
            "kerberos-admin-hostname"
            ]
        c = 0
        for e in mandatory_options:
            if len(self.config.get(e, None)) == 0:
                c += 1
        if c == len(mandatory_options):
            # None of the mandatory items are set, it means
            # the operator does not want it
            return False
        if c == 0:
            # All options are set, we can return True
            return True
        # There are some items unset, warn:
        for e in mandatory_options:
            if len(self.config.get(e, None)) == 0:
                raise KafkaCharmBaseMissingConfigError(e)
        return False

    def is_sasl_digest_enabled(self):
        # TODO(pguimaraes): implement this logic
        return False

    def is_jolokia_enabled(self):
        # TODO(pguimaraes): implement this logic
        return False

    def is_jmxexporter_enabled(self):
        return False

    def _get_confluent_ldap_jaas_config(self,
                                        mds_user, mds_password, mds_urls):
        return 'org.apache.kafka.common.security.oauthbearer.' + \
            'OAuthBearerLoginModule required ' + \
            'username={} password={} '.format(mds_user, mds_password) + \
            'metadataServerUrls="{}"'.format(mds_urls)

    def _on_install(self, event):
        try:
            groupAdd(self.config["group"], system=True)
        except LinuxGroupAlreadyExistsError:
            pass
        try:
            home = "/home/{}".format(self.config["user"])
            userAdd(
                self.config["user"],
                home=home,
                group=self.config["group"])
            os.makedirs(home, 0o755, exist_ok=True)
            shutil.chown(home,
                         user=self.config["user"],
                         group=self.config["group"])
        except LinuxUserAlreadyExistsError:
            pass

    def create_log_dir(self, data_log_dev,
                       data_log_dir,
                       data_log_fs,
                       user="cp-kafka",
                       group="confluent",
                       fs_options=None):

        if len(data_log_dir or "") == 0:
            logger.warning("Data log dir config empty")
            BlockedStatus("data-log-dir missing, please define it")
            return
        os.makedirs(data_log_dir, 0o750, exist_ok=True)
        shutil.chown(data_log_dir,
                     user=self.config["user"],
                     group=self.config["group"])
        dev, fs = None, None
        if len(data_log_dev or "") == 0:
            logger.warning("Data log device not found, using rootfs instead")
        else:
            for k, v in data_log_dev:
                fs = k
                dev = v
            logger.info("Data log device: mkfs -t {}".format(fs))
            cmd = ["mkfs", "-t", fs, dev]
            subprocess.check_call(cmd)
            mount(dev, data_log_dir,
                  options=self.config.get("fs-options", None),
                  persist=True, filesystem=fs)

    def create_data_and_log_dirs(self, data_log_dev,
                                 data_dev,
                                 data_log_dir,
                                 data_dir,
                                 data_log_fs,
                                 data_fs,
                                 user="cp-kafka",
                                 group="confluent",
                                 fs_options=None):

        if len(data_log_dir or "") == 0:
            logger.warning("Data log dir config empty")
            BlockedStatus("data-log-dir missing, please define it")
            return
        if len(data_dir or "") == 0:
            logger.warning("Data dir config empty")
            BlockedStatus("data-dir missing, please define it")
            return
        os.makedirs(data_log_dir, 0o750, exist_ok=True)
        shutil.chown(data_log_dir,
                     user=self.config["user"],
                     group=self.config["group"])
        os.makedirs(data_dir, 0o750, exist_ok=True)
        shutil.chown(data_dir,
                     user=self.config["user"],
                     group=self.config["group"])
        dev, fs = None, None
        if len(data_log_dev or "") == 0:
            logger.warning("Data log device not found, using rootfs instead")
        else:
            for k, v in data_log_dev.items():
                fs = k
                dev = v
            logger.info("Data log device: mkfs -t {}".format(fs))
            cmd = ["mkfs", "-t", fs, dev]
            subprocess.check_call(cmd)
            mount(dev, data_log_dir,
                  options=self.config.get("fs-options", None),
                  persist=True, filesystem=fs)

        if len(data_dev or "") == 0:
            logger.warning("Data device not found, using rootfs instead")
        else:
            for k, v in data_dev.items():
                fs = k
                dev = v
            logger.info("Data log device: mkfs -t {}".format(fs))
            cmd = ["mkfs", "-t", fs, dev]
            subprocess.check_call(cmd)
            mount(dev, data_dir,
                  options=self.config.get("fs-options", None),
                  persist=True, filesystem=fs)

    # To be used on parameter: confluent.license.topic
    def get_license_topic(self):
        if self.distro == "confluent":
            # If unset, return it empty
            return self.config.get("confluent_license_topic")
        return None

    def render_service_override_file(
            self, target,
            jmx_jar_folder="/opt/prometheus/",
            jmx_file_name="/opt/prometheus/prometheus.yaml",
            extra_envvars=None):
        """Renders the service override.conf file.

        In the snap deployment, jmx will be placed on a /snap and
        prometheus.yaml will be placed on /var/snap.

        target: filepath for the override file
        jmx_jar_folder: either /opt on confluent or /snap on apache_snap
        jmx_file_name: either /opt on confluent or /var/snap for snap
        extra_envvars: allows a charm to specify if any additional env
                       vars should be added. This value will take priority
                       over the config options and any configs set by this
                       class.
        """

        service_unit_overrides = yaml.safe_load(
            self.config.get('service-unit-overrides', ""))
        service_overrides = yaml.safe_load(
            self.config.get('service-overrides', ""))
        service_environment_overrides = yaml.safe_load(
            self.config.get('service-environment-overrides', ""))

        if "KAFKA_OPTS" not in service_environment_overrides:
            # Assume it will be needed, so adding it
            service_environment_overrides["KAFKA_OPTS"] = ""
        kafka_opts = []
        if self.is_ssl_enabled():
            kafka_opts.append("-Djdk.tls.ephemeralDHKeySize=2048")
        if self.is_sasl_enabled():
            kafka_opts.append(
                "-Djava.security.auth.login.config="
                "/etc/kafka/jaas.conf")
        if self.is_jolokia_enabled():
            kafka_opts.append(
                "-javaagent:/opt/jolokia/jolokia.jar="
                "config=/etc/kafka/jolokia.properties")
        if self.is_jmxexporter_enabled():
            kafka_opts.append(
                "-javaagent:{}={}:{}"
                .format(
                    jmx_jar_folder + self.JMX_EXPORTER_JAR_NAME,
                    self.config.get("jmx-exporter-port", 9404),
                    jmx_file_name))
            render(source="prometheus.yaml",
                   target=jmx_file_name,
                   owner=self.config.get('user'),
                   group=self.config.get("group"),
                   perms=0o644,
                   context={})
        if len(kafka_opts) == 0:
            # Assumed KAFKA_OPTS would be set at some point
            # however, it was not, so removing it
            service_environment_overrides.pop("KAFKA_OPTS", None)
        else:
            service_environment_overrides["KAFKA_OPTS"] = \
                '{}'.format(" ".join(kafka_opts))
            # Now, ensure all the kafka charms have their OPTS set:
            if "SCHEMA_REGISTRY_OPTS" not in service_environment_overrides:
                service_environment_overrides["SCHEMA_REGISTRY_OPTS"] = \
                    service_environment_overrides["KAFKA_OPTS"]
            if "KSQL_OPTS" not in service_environment_overrides:
                service_environment_overrides["KSQL_OPTS"] = \
                    service_environment_overrides["KAFKA_OPTS"]
            if "KAFKAREST_OPTS" not in service_environment_overrides:
                service_environment_overrides["KAFKAREST_OPTS"] = \
                    service_environment_overrides["KAFKA_OPTS"]
            if "CONTROL_CENTER_OPTS" not in service_environment_overrides:
                service_environment_overrides["CONTROL_CENTER_OPTS"] = \
                    service_environment_overrides["KAFKA_OPTS"]
        if extra_envvars:
            for k, v in extra_envvars.items():
                service_environment_overrides[k] = v

        # Even if service_overrides is not defined, User and Group need to be
        # correctly set if this option was passed to the charm.
        if not service_overrides:
            service_overrides = {}
        for d in ["User", "Group"]:
            dlower = d.lower()
            if dlower in self.config and \
               len(self.config.get(dlower, "")) > 0:
                service_overrides[d] = self.config.get(dlower)
        self.set_folders_and_permissions([os.path.dirname(target)])
        render_from_string(source=OVERRIDE_CONF,
               target=target,
               owner=self.config.get('user'),
               group=self.config.get("group"),
               perms=0o644,
               context={
                   "service_unit_overrides": service_unit_overrides or {},
                   "service_overrides": service_overrides or {},
                   "service_environment_overrides": service_environment_overrides or {} # noqa
               })
        # Shortening the name
        svc_override = service_environment_overrides
        return {
            "service_unit_overrides": service_unit_overrides or {},
            "service_overrides": service_overrides or {},
            "service_environment_overrides": svc_override or {},
            "is_jmx_exporter_enabled": self.is_jmxexporter_enabled()
        }

    def _render_jaas_conf(self, jaas_path="/etc/kafka/jaas.conf"):
        content = ""
        if self.is_sasl_kerberos_enabled():
            krb = """Server {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="/etc/security/keytabs/{}"
    storeKey=true
    useTicketCache=false
    principal="{}";
}};
""".format(self.keytab, self.kerberos_principal) # noqa
            content += krb
        self.set_folders_and_permissions([os.path.dirname(jaas_path)])
        with open(jaas_path, "w") as f:
            f.write(content)
        setFilePermissions(jaas_path, self.config.get("user", "root"),
                           self.config.get("group", "root"), 0o640)
        return content

    def _render_krb5_conf(self):
        enctypes = "aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96" + \
            " arc-four-hmac rc4-hmac"
        ctx = {
                "realm": self.config["kerberos-realm"],
                "dns_lookup_realm": "false",
                "dns_lookup_kdc": "false",
                "ticket_lifetime": "24h",
                "forwardable": "true",
                "udp_preference_limit": "1",
                "default_tkt_enctypes": enctypes,
                "default_tgs_enctypes": enctypes,
                "permitted_enctypes": enctypes,
                "kdc_hostname": self.config["kerberos-kdc-hostname"],
                "admin_hostname": self.config["kerberos-admin-hostname"]
            }
        render_from_string(source=KRB5_CONF,
               target="/etc/krb5.conf",
               owner=self.config.get("user", "root"),
               group=self.config.get("group", "root"),
               perms=0o640,
               context=ctx)
        return ctx

    def _on_config_changed(self, event):
        """Implements the JAAS logic. Returns changes in config files.
        """

        changed = {}
        if self.is_sasl_kerberos_enabled():
            changed["krb5_conf"] = self._render_krb5_conf()
            changed["jaas"] = self._render_jaas_conf()
        return changed
