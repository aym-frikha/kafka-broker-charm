import os
import socket
from wand.contrib.linux import get_hostname

from wand.apps.relations.kafka_relation_base import KafkaRelationBase


class KafkaBrokerCluster(KafkaRelationBase):

    def __init__(self, charm, relation_name, min_units=3):
        super().__init__(charm, relation_name)
        self.state.set_default(peer_num_azs=0)
        self.state.set_default(listeners="")
        self.state.set_default(listener_protocol_map="")
        self.state.set_default(advertised_listeners="")
        self._min_units = min_units
        self._enable_az = False

    @property
    def min_units(self):
        return self._min_units

    @min_units.setter
    def min_units(self, u):
        self._min_units = u

    @property
    def enable_az(self):
        return self._enable_az

    @enable_az.setter
    def enable_az(self, x):
        self._enable_az = x

    @property
    def is_ready(self):
        if not self.relation or self.min_units == 1:
            return True
        if len(self.all_units(self.relation)) < self.min_units:
            return False
        return True

    def set_ssl_keypair(self,
                        ssl_cert,
                        ts_path,
                        ts_pwd,
                        user, group, mode):
        self.state.user = user
        self.state.group = group
        self.state.mode = mode
        # Cluster will not manage the keys anymore
        # self.set_TLS_auth(ssl_cert, ts_path, ts_pwd)

    @property
    def truststore_pwd(self):
        return self.state.ts_pwd

    @property
    def truststore(self):
        return self.state.ts_path

    def _get_all_tls_certs(self):
        super()._get_all_tls_cert()

    @property
    def num_peers(self):
        return len(self.all_units(self.relation))

    @property
    def num_azs(self):
        if not self._charm.config["customize-failure-domain"]:
            return 0
        return self.state.peer_num_azs

    def listener_opts(self,
                      keystore_path, keystore_pwd, keystore_type="JKS", clientauth=False):
        # DEPRECATED METHOD
        listener_opts = {
            "listeners": self.state.listeners,
            "listener.security.protocol.map": self.state.listener_protocol_map,
            "advertised.listeners": self.state.advertised_listeners,
            "inter.broker.listener.name": "internal",
        }
        if self.is_TLS_enabled():
            ssl_opts = {}
            for lst in self.state.listeners.split(","):
                name = "listener.name." + lst.split("://")[0]
                _opts = {}
                _opts[name + ".ssl.truststore.location"] = \
                    self.state.ts_path
                _opts[name + ".ssl.truststore.password"] = \
                    self.state.ts_pwd
                _opts[name + ".ssl.truststore.type"] = "JKS"
                _opts[name + ".ssl.keystore.location"] = \
                    keystore_path
                _opts[name + ".ssl.keystore.password"] = \
                    keystore_pwd
                _opts[name + ".ssl.keystore.type"] = keystore_type
                _opts[name + ".ssl.client.auth"] = "required" if clientauth else "none"
                ssl_opts = {**_opts, **ssl_opts}
            listener_opts = {**ssl_opts, **listener_opts}
        return listener_opts

    @property
    def is_single(self):
        return len(self.relation) == 1

    @property
    def is_joined(self):
        return self._relation is not None

    def on_cluster_relation_joined(self, event):
        self.on_cluster_relation_changed(event)

    @property
    def hostname(self):
        return get_hostname(self.binding_addr)

    def _get_hostname(self, listener):
        clusterdomain = "{}-cluster-domain".format(listener.lower())
        if clusterdomain in self.charm.config:
            return "{}.{}".format(
                socket.gethostname(),
                self.charm.config[clusterdomain])
        return get_hostname(self.binding_addr())

    def on_cluster_relation_changed(self, event):
        self._get_all_tls_certs()

        if self.enable_az:
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
        listeners["internal"] = \
            "{}:{}".format(self.hostname, 9092)
        listeners["broker"] = \
            "{}:{}".format(self.hostname, 9093)
        listeners["client"] = \
            "{}:{}".format(get_hostname(self.advertise_addr), 9094)
        self.state.listeners = \
            ",".join([k+"://"+v for k, v in list(listeners.items())])
        # TODO: avoid PLAINTEXT, check:
        # https://docs.confluent.io/platform/current/installation/ \
        #     configuration/broker-configs.html#  \
        #     brokerconfigs_listener.security.protocol.map
        # TODO: add SASL if available
        if not self.is_TLS_enabled():
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
