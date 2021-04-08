import os
import yaml
from ops.framework import Object, StoredState

class KafkaBrokerCluster(Object):

    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)
        self.framework.observe(charm.on.cluster_relation_changed, self)
        self.framework.observe(charm.on.cluster_relation_joined, self)
        self.state.set_default(peer_num_azs=0)
        self.state.set_default(listeners="")
        self.state.set_default(listener_protocol_map="")
        self.state.set_default(advertised_listeners="")


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
        return {
            "listeners": self.state.listeners,
            "listener.security.protocol.map": self.state.listener_protocol_map,
            "advertised.listeners": self.state.advertised_listeners,
            "inter.broker.listener.name": "INTERNAL",
            "control.plane.listener.name": "CONTROLLER"
        }

    @property
    def is_single(self):
        return len(self._relations) == 1

    @property
    def is_joined(self):
        return self._relation is not None

    def on_cluster_relation_joined(self, event):
        # A workaround for LP: #1859769.
        if not self.is_joined:
            event.defer()
            return
        # TODO: keep this method, we will likely needed it for later

    def on_cluster_relation_changed(self, event):
        if os.environ.get("JUJU_AVAILABILITY_ZONE"):
            self._relation.data[self.charm.unit]["az"] = os.environ.get("JUJU_AVAILABILITY_ZONE")
        az_set = set()
        for u in self._relation.units:
            az_set.add(self._relation.data[u]["az"])
        self.state.peer_num_azs = len(az_set)

        # TODO: ssl_enabled and sasl: add to the listeners
        ssl_enabled = True if len(self._charm.config.get("ssl_cert") > 0 else False
        sasl_protocol = self._charm.config.get("sasl_protocol", False)
        # Creates a list similar to: [{'test': [{'a': 'b', 'c': 'd'}]}]
        listeners = yaml.safe_load(self._charm.config.get("listeners",""))[0] or {}
        listeners["INTERNAL"] =   "{}:{}".format(self.binding_addr(), 9092)
        listeners["CONTROLLER"] = "{}:{}".format(self.binding_addr(), 9093)
        listeners["EXTERNAL"] =   "{}:{}".format(self.advertise_addr(),9094)
        self.state.listeners = ",".join( [ k+"://"+v for k,v in listeners ] )
        # TODO: get a second advertised listener list
        self.state.adversited_listeners = ",".join( [ k+"://"+v for k,v in listeners ] )
        # TODO: avoid PLAINTEXT
        self.state.listener_protocol_map = ",".join( [ k+":PLAINTEXT" for k,v in listeners ] )

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
