import os
import yaml
from socket import gethostname
from ops.framework import Object, StoredState
from charmhelpers.contrib.network.ip import get_hostname

class KafkaBrokerCluster(Object):

    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._unit  = unit
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)
        self.framework.observe(charm.on.cluster_relation_changed, self)
        self.framework.observe(charm.on.cluster_relation_joined, self)
        self.state.set_default(peer_num_azs=0)
        self.state.set_default(listeners="")
        self.state.set_default(listener_protocol_map="")
        self.state.set_default(advertised_listeners="")
        self.state.set_default(keystore_path="/var/lib/private/kafka-broker-ssl-keystore.jks")
        self.state.set_default(keystore_pwd="")
        self.state.set_default(truststore_path="/var/ssl/private/kafka-broker-ssl-truststore.jks")
        self.state.set_default(truststore_pwd="")
        self.state.set_default(trusted_certs="")


    def set_ssl_keypair(self,
                        ssl_cert,
                        ks_path,
                        ks_pwd):
        self._relation.data[self._unit]["tls_cert"] = ssl_cert
        self.state.keystore_path = ks_path
        self.state.keystore_pwd  = ks_pwd
        self._get_all_tls_certs()

    def is_ssl_enabled(self):
        if len(self._relation.data[self._unit].get("tls_cert", "")) > 0:
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
        self.state.trusted_certs = self._relation.data[self._unit]["tls_cert"] + \
            " " + " ".join(
                [self._relation.data[u].get("tls_cert", "") for u in self._relation.units])
        CreateTruststore(self.state.truststore_path,
                         self.state.truststore_pwd,
                         self.state.trusted_certs.split(),
                         ts_regenerate=True)

    @property
    def _relations(self):
        return self.framework.model.relations[self._relation_name]

    @property
    def number_peers(self):
        return len(self._relations)

    @property
    def number_azs(self):
        if not self.charm.config["customize-failure-domain"]:
            return 0
        return self.state.peer_num_azs

    @property
    def listeners(self):
        listener_opts = {
            "listeners": self.state.listeners,
            "listener.security.protocol.map": self.state.listener_protocol_map,
            "advertised.listeners": self.state.advertised_listeners,
            "inter.broker.listener.name": "INTERNAL",
            "control.plane.listener.name": "CONTROLLER"
        }
        if is_ssl_enabled():
            ssl_opts = {}
            for l in self.state.listeners.split(","):
                name = "listener.name." + l.split("://")[0]
                _opts = {}
                _opts[name + ".ssl.truststore.location"] = self.state.truststore_path
                _opts[name + ".ssl.truststore.password"] = self.state.truststore_pwd
                _opts[name + ".ssl.truststore.typee"] = "JKS"
                _opts[name + ".ssl.keystore.location"] = self.state.keystore_path
                _opts[name + ".ssl.keystore.password"] = self.state.keystore_pwd
                _opts[name + ".ssl.keystore.typee"] = "JKS"
                _pots[name + ".ssl.client.auth"] = "required"
                ssl_opts = { **_opts, **ssl_opts }
            listener_opts = { **ss_opts, **listener_opts }

    @property
    def is_single(self):
        return len(self._relations) == 1

    @property
    def is_joined(self):
        return self._relation is not None

    def on_cluster_relation_joined(self, event):
        pass

    def on_cluster_relation_changed(self, event):
        if os.environ.get("JUJU_AVAILABILITY_ZONE"):
            self._relation.data[self.charm.unit]["az"] = os.environ.get("JUJU_AVAILABILITY_ZONE")
        az_set = set()
        for u in self._relation.units:
            az_set.add(self._relation.data[u]["az"])
        self.state.peer_num_azs = len(az_set)
        # Resolve hostname
        hostname = "{}.{}".format(socket.gethostname(), self.config["cluster-domain"]) \
                   if self.config["cluster-domain"] else get_hostname(self.binding_addr())
        # TODO: ssl_enabled and sasl: add to the listeners
        ssl_enabled = self.is_ssl_enabled()
        sasl_protocol = self._charm.config.get("sasl_protocol", False)
        # Creates a list similar to: [{'test': [{'a': 'b', 'c': 'd'}]}]
        listeners = self._charm.config.get("listeners","") or {}
        # If this is set via option, we override those values to predefined on charms
        listeners["INTERNAL"] =   "{}:{}".format(hostname, 9092)
        listeners["CONTROLLER"] = "{}:{}".format(hostname, 9093)
        listeners["EXTERNAL"] =   "{}:{}".format(hostname, 9094)
        self.state.listeners = ",".join( [ k+"://"+v for k,v in listeners ] )
        # TODO: avoid PLAINTEXT, check:
        # https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#brokerconfigs_listener.security.protocol.map
        if not ssl_enabled:
            self.state.listener_protocol_map = ",".join( [ k+":PLAINTEXT" for k,v in listeners ] )
        else:
            self.state.listener_protocol_map = ",".join( [ k+":SSL" for k,v in listeners ] )
        # if advertised listeners is set, pass it here
        if self._charm.config.get("advertised.listeners", None):
            self.state.advertised_listeners = self.state.advertised_listeners + \
                self._charm.config["advertised.listeners"]
        else:
            self.state.advertised_listeners = self.state.listeners["EXTERNAL"]

    @property
    def peer_addresses(self):
        addresses = []
        for u in self._relation.units:
            addresses.append(self._relation.data[u]["ingress-address"]
        return addresses

    @property
    def advertise_addr(self):
        return self.model.get_binding(self._relation_name).network.ingress_address

    @property
    def binding_addr(self):
        return self.model.get_binding(self._relation_name).network.binding_address
