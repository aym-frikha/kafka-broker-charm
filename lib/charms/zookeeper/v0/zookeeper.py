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

from charms.kafka_broker.v0.kafka_linux import get_hostname

from charms.kafka_broker.v0.kafka_relation_base import KafkaRelationBase

__all__ = [
    'ZookeeperRelation',
    'ZookeeperProvidesRelation',
    'ZookeeperRequiresRelation'
]


class ZookeeperRelation(KafkaRelationBase):

    def __init__(self, charm, relation_name, hostname=None,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)
        self._hostname = hostname
        self._charm = charm
        self._unit = charm.unit
        self._relation_name = relation_name
        self.state.set_default(zk_list="")

    @property
    def hostname(self):
        return self._hostname if self._hostname \
            else get_hostname(self.binding_addr)

    @property
    def get_zookeeper_list(self):
        return self.state.zk_list

    # TODO(pguimaraes): complete this method. So far returns False as it is
    # not an option atm
    def is_sasl_enabled(self):
        return False

    def client_auth_enabled(self):
        return self.is_TLS_enabled() or self.is_sasl_enabled()

    def set_mTLS_auth(self,
                      cert_chain,
                      truststore_path,
                      truststore_pwd,
                      user=None,
                      group=None,
                      mode=None):
        self.set_TLS_auth(
            cert_chain, truststore_path, truststore_pwd,
            user=user, group=group, mode=mode)

    def on_zookeeper_relation_joined(self, event):
        pass

    def on_zookeeper_relation_changed(self, event):
        zk_list = []
        r = event.relation
        for u in r.units:
            if "endpoint" not in r.data[u]:
                continue
            if len(r.data[u]["endpoint"]) == 0:
                continue
            zk_list.append(r.data[u]["endpoint"])
        self.state.zk_list = ",".join(zk_list)
        self._get_all_tls_cert()


class ZookeeperProvidesRelation(ZookeeperRelation):

    def __init__(self, charm, relation_name, hostname=None, port=2182,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, hostname=hostname,
                         user=user, group=group, mode=mode)
        self._hostname = hostname
        self._port = port

    def enable_sasl_kerberos(self):
        if not self.relations:
            return
        if not self.unit.is_leader():
            return
        for r in self.relations:
            # Only change the value if it is not yet set
            if r.data[self.model.app].get(
               "sasl_kerberos_enabled", "false") != "true":
                r.data[self.model.app]["sasl_kerberos_enabled"] = "true"

    def disable_sasl_kerberos(self):
        if not self.relations:
            return
        if not self.unit.is_leader():
            return
        for r in self.relations:
            # Only change the value if it is not yet set
            if r.data[self.model.app].get(
               "sasl_kerberos_enabled", "false") != "false":
                r.data[self.model.app]["sasl_kerberos_enabled"] = "false"

    def on_zookeeper_relation_joined(self, event):
        # Get unit's own hostname and pass that via relation
        r = event.relation
        hostname = self._hostname if self._hostname \
            else get_hostname(self.binding_addr)
        r.data[self.unit]["endpoint"] = \
            "{}:{}".format(hostname, self._port)

    def on_zookeeper_relation_changed(self, event):
        # First, update this unit entry for "endpoint"
        self.on_zookeeper_relation_joined(event)
        # Second, recover data from peers
        # Third, check if tls_cert has been set. If so, check
        # if any of the other peers have also published tls for the
        # truststore.
        # This is done already on parent class method
        super().on_zookeeper_relation_changed(event)


class ZookeeperRequiresRelation(ZookeeperRelation):

    def __init__(self, charm, relation_name, hostname=None,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, hostname=hostname,
                         user=user, group=group, mode=mode)

    def is_sasl_kerberos_enabled(self):
        if not self.relations:
            return False
        for r in self.relations:
            if r.data[r.app].get("sasl_kerberos_enabled", "false") == "true":
                return True
        return False
