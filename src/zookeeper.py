from ops.framework import Object, StoredState

from charmhelpers.contrib.network.ip import (
    get_hostname
)


class ZookeeperRelation(Object):
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._unit = charm.unit
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)
        self.state.set_default(zk_list="")

    @property
    def _relations(self):
        return self.framework.model.relations[self._relation_name]

    @property
    def get_zookeeper_list(self):
        return self.state.zk_list

    @property
    def advertise_addr(self):
        return self.model.get_binding(self._relation_name).network.ingress_address

    def on_zookeeper_relation_joined(self, event):
        pass

    def on_zookeeper_relation_changed(self, event):
        zk_list = []
        for u in self._relation.units:
            if not self._relation.data[u]["endpoint"]:
                continue
            if len(self._relation.data[u]["endpoint"]) == 0:
                continue
            zk_list.append(self._relation.data[u]["endpoint"])
        self.state.zk_list = ",".join(zk_list)


class ZookeeperProvidesRelation(ZookeeperRelation):

    def __init__(self, charm, relation_name, port):
        super().__init__(charm, relation_name)
        self.framework.observe(charm.on.zookeeper_relation_changed, self)
        self.framework.observe(charm.on.zookeeper_relation_joined, self)
        self._port = port

    def on_zookeeper_relation_joined(self, event):
        # Get unit's own hostname and pass that via relation
        self._relations[self._unit]["endpoint"] = \
            "{}:{}".format(get_hostname(self.advertise_addr),
                           self._port)

    def on_zookeeper_relation_changed(self, event):
        # First, update this unit entry for "endpoint"
        self.on_zookeper_relation_joined(event)
        # Second, recover data from peers
        super().on_zookeeper_relation_changed(event)


# TODO: Generate the: zookeeper-tls-client.properties.j2
class ZookeeperRequiresRelation(ZookeeperRelation):

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.framework.observe(charm.on.zookeeper_relation_changed, self)

