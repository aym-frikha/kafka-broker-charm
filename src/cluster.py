import os
import socket
from ops.framework import Object, StoredState
from charmhelpers.contrib.network.ip import get_hostname

from wand.security.ssl import (
    CreateTruststore
)

class KafkaBrokerCluster(Object):

    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._unit = charm.unit
        self._relation_name = relation_name
        self._relation = \
            self.framework.model.get_relation(self._relation_name)
        self.framework.observe(
            charm.on.cluster_relation_changed,
            self.on_cluster_relation_changed)
        self.framework.observe(
            charm.on.cluster_relation_joined,
            self.on_cluster_relation_joined)
        self.state.set_default(peer_num_azs=0)
        self.state.set_default(listeners="")
        self.state.set_default(listener_protocol_map="")
        self.state.set_default(advertised_listeners="")
        self.state.set_default(
            keystore_path="/var/lib/private/kafka-broker-ssl-keystore.jks")
        self.state.set_default(keystore_pwd="")
        self.state.set_default(
            truststore_path="/var/ssl/private/kafka-broker-ssl-truststore.jks")
        self.state.set_default(truststore_pwd="")
        self.state.set_default(trusted_certs="")
        self.state.set_default(truststore_user="")
        self.state.set_default(truststore_group="")
        self.state.set_default(truststore_mode="")

    @property
    def is_ready(self):
        return True

    @property
    def user(self):
        return self.state.truststore_user

    @user.setter
    def user(self, u):
        self.state.truststore_user = u

    @property
    def group(self):
        return self.state.truststore_group

    @group.setter
    def group(self, g):
        self.state.truststore_group = g

    @property
    def mode(self):
        return self.state.truststore_mode

    @mode.setter
    def mode(self, m):
        self.state.truststore_mode = m

    @property
    def relation(self):
        return self._relation

    @property
    def unit(self):
        return self._unit

    @property
    def charm(self):
        return self._charm

    def set_ssl_keypair(self,
                        ssl_cert,
                        ks_path,
                        ks_pwd,
                        ts_path,
                        ts_pwd,
                        user, group, mode):
        self.relation.data[self.unit]["tls_cert"] = ssl_cert
        self.state.keystore_path = ks_path
        self.state.keystore_pwd = ks_pwd
        self.state.truststore_user = user
        self.state.truststore_group = group
        self.state.truststore_mode = mode
        self.state.truststore_path = ts_path
        self.state.truststore_pwd = ts_pwd
        self._get_all_tls_certs()

    def is_ssl_enabled(self):
        if len(self.relation.data[self.unit].get("tls_cert", "")) > 0:
            return True
        return False

    @property
    def keystore(self):
        return self.state.keystore_path

    @property
    def keystore_pwd(self):
        return self.state.keystore_pwd

    @property
    def truststore_pwd(self):
        return self.state.truststore_pwd

    @property
    def truststore(self):
        return self.state.truststore_path

    def _get_all_tls_certs(self):
        if not self.is_ssl_enabled():
            return
        self.state.trusted_certs = \
            "::".join(list(self.relation.data[u].get("tls_cert", "")
                           for u in self.relation.units))
        CreateTruststore(self.state.truststore_path,
                         self.state.truststore_pwd,
                         self.state.trusted_certs.split("::"),
                         ts_regenerate=True,
                         user=self.state.truststore_user,
                         group=self.state.truststore_group,
                         mode=self.state.truststore_mode)

    @property
    def relations(self):
        return self.framework.model.relations[self._relation_name]

    @property
    def num_peers(self):
        return len(self._relations)

    @property
    def num_azs(self):
        if not self._charm.config["customize-failure-domain"]:
            return 0
        return self.state.peer_num_azs

    @property
    def listener_opts(self):
        listener_opts = {
            "listeners": self.state.listeners,
            "listener.security.protocol.map": self.state.listener_protocol_map,
            "advertised.listeners": self.state.advertised_listeners,
            "inter.broker.listener.name": "internal",
        }
        if self.is_ssl_enabled():
            ssl_opts = {}
            for lst in self.state.listeners.split(","):
                name = "listener.name." + lst.split("://")[0]
                _opts = {}
                _opts[name + ".ssl.truststore.location"] = \
                    self.state.truststore_path
                _opts[name + ".ssl.truststore.password"] = \
                    self.state.truststore_pwd
                _opts[name + ".ssl.truststore.typee"] = "JKS"
                _opts[name + ".ssl.keystore.location"] = \
                    self.state.keystore_path
                _opts[name + ".ssl.keystore.password"] = \
                    self.state.keystore_pwd
                _opts[name + ".ssl.keystore.typee"] = "JKS"
                _opts[name + ".ssl.client.auth"] = "required"
                ssl_opts = {**_opts, **ssl_opts}
            listener_opts = {**ssl_opts, **listener_opts}
        return listener_opts

    @property
    def is_single(self):
        return len(self._relations) == 1

    @property
    def is_joined(self):
        return self._relation is not None

    def on_cluster_relation_joined(self, event):
        self.on_cluster_relation_changed(event)

    def _get_hostname(self, listener):
        clusterdomain = "{}-cluster-domain".format(listener.lower())
        if clusterdomain in self.charm.config:
            return "{}.{}".format(
            socket.gethostname(),
            self.charm.config[clusterdomain])
        return get_hostname(self.binding_addr())

    def on_cluster_relation_changed(self, event):
        self._get_all_tls_certs()

        if os.environ.get("JUJU_AVAILABILITY_ZONE"):
            self.relation.data[self.unit]["az"] = \
                os.environ.get("JUJU_AVAILABILITY_ZONE")
        az_set = set()
        for u in self.relation.units:
            az_set.add(self.relation.data[u]["az"])
        self.state.peer_num_azs = len(az_set)
        # Creates a list similar to: [{'test': [{'a': 'b', 'c': 'd'}]}]
        listeners = self._charm.config.get("listeners", "") or {}
        # If this is set via option, we override
        # those values to predefined on charms
        listeners["internal"] = "{}:{}".format(self._get_hostname("internal"), 9092)
        listeners["broker"] = "{}:{}".format(self._get_hostname("broker"), 9093)
        listeners["client"] = "{}:{}".format(self._get_hostname("client"), 9094)
        self.state.listeners = ",".join([k+"://"+v for k, v in list(listeners.items())])
        # TODO: avoid PLAINTEXT, check:
        # https://docs.confluent.io/platform/current/installation/ \
        #     configuration/broker-configs.html#  \
        #     brokerconfigs_listener.security.protocol.map
        # TODO: add SASL if available
        if not self.is_ssl_enabled():
            self.state.listener_protocol_map = \
                ",".join([k+":PLAINTEXT" for k, v in list(listeners.items())])
        else:
            self.state.listener_protocol_map = \
                ",".join([k+":SSL" for k, v in list(listeners.items())])
        # if advertised listeners is set, pass it here
        if self._charm.config.get("advertised.listeners", None):
            self.state.advertised_listeners = \
                self.state.advertised_listeners + \
                self._charm.config["advertised.listeners"]
        else:
            self.state.advertised_listeners = self.state.listeners

    @property
    def peer_addresses(self):
        addresses = []
        for u in self.relation.units:
            addresses.append(self.relation.data[u]["ingress-address"])
        return addresses

    @property
    def advertise_addr(self):
        m = self.model
        return m.get_binding(self._relation_name).network.ingress_address

    @property
    def binding_addr(self):
        m = self.model
        return m.get_binding(self._relation_name).network.binding_address
