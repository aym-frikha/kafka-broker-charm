import os
from ops.framework import Object, StoredState

class KafkaBrokerCluster(Object):

    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)
        self.framework.observe(charm.on.cluster_relation_changed, self)
        self.framework.observe(charm.on.cluster_relation_joined, self)
        self.state.set_default(peer_num_azs=None)

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
            az_set.add(self._relation.data[u]["azs"])
        self.state.peer_num_azs = len(az_set)

    @property
    def peer_addresses(self):
        addresses = []
        for u in self._relation.units:
            addresses.append(self._relation.data[u]['ingress-address'])
        return addresses

    @property
    def advertise_addr(self):
        return self.model.get_binding(self._relation_name).network.ingress_address
