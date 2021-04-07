from ops.framework import Object, StoredState


class ZookeeperRelation(Object):

    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name):
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)
        self.framework.observe(charm.on.zookeeper_relation_changed, self)
        self.framework.observe(charm.on.zookeeper_relation_joined, self)
        self.state.set_default(zk_list="")

    @property
    def _relations(self):
        return self.framework.model.relations[self._relation_name]

    @property
    def get_zookeeper_list(self):
        return self.state.zk_list

    def on_zookeeper_relation_joined(self, event):
        pass

    def on_zookeeper_relation_changed(self, event):
        zk_list = []
        for u in self._relation.units:
            port = self._relation.data[u].get('port', "2181")
            zk_list.append("{}:{}".format(self._relation.data[u]['ingress-address'],port))
        self.state.zk_list = ",".join(zk_list)
