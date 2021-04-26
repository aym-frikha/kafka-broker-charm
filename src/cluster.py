import os
import socket
import json
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

    def set_ssl_cert(self,
                     ssl_cert):
        if self.relation:
            if ssl_cert != self.relation.data[self.unit].get("cert", ""):
                self.relation.data[self.unit]["cert"] = ssl_cert

    def get_all_certs(self):
        crt_list = []
        for r in self.relations:
            for u in r.units:
                if "cert" in r.data[u]:
                    crt_list.append(r.data[u]["cert"])
        return crt_list

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

    def set_listeners(self, listeners):
        if not self.unit.is_leader() or not self.relation:
            return
        if listeners != self.relation.data[self.model.app].get("listeners", "{}"):
            self.relation.data[self.model.app]["listeners"] = listeners
#        if listeners != json.loads(self.relation.data[self.model.app].get("listeners", "{}")):
#            self.relation.data[self.model.app]["listeners"] = json.dumps(listeners)

    def get_listener_template(self):
        return self.relation.data[self.model.app].get("listeners", "")
#            return {}
#        return json.loads(self.relation.data[self.model.app]["listeners"])

    def listener_opts(self,
                      keystore_path, keystore_pwd, keystore_type="JKS", clientauth=False):
        # DEPRECATED METHOD
        return

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
