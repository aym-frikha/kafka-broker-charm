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

Implements the Metadata service logic from Confluent.

Once the relation is joined, the Requirers will generate the requests
and send to the MDS servers.

Units should check if RBAC is set in all applications.
This feature only makes sense if it is set for all the element.
Therefore, if one of the applications do not have RBAC set, then
an exception is raised.

MDS relation can be cross-model in a centralized user management
scenario with multiple clusters.

"""

import json

from charms.kafka_broker.v0.kafka_relation_base import (
    KafkaRelationBase
)
from charms.kafka_broker.v0.kafka_linux import (
    get_hostname,
    send_request
)

__all__ = [
    "KafkaMDSRelation",
    "KafkaMDSProvidesRelation",
    "KafkaMDSRequiresRelation",
    "KafkaSchemaRegistryMDSRequiresRelation",
    "KafkaConnectMDSRequiresRelation",
    "KafkaMDSRelationConfigIncorrectError",
    "KafkaMDSRelationRBACNotSetError",
    "KafkaMDSRequiresRelationMissingJsonRequestParamError"
]


class KafkaMDSRelationValueNeededNotSetError(Exception):

    def __init__(self, v):
        super().__init__("Following value is needed but "
                         "not yet set by the other side: {}".format(v))


class KafkaMDSRelationConfigIncorrectError(Exception):

    def __init__(self, c):
        super().__init__("Config set incorrectly: {}".format(c))


class KafkaMDSRelationRBACNotSetError(Exception):

    def __init__(self, r, message="RBAC set only on some "
                                  "units of the relation "
                                  "{}, but not this one"):
        super().__init__(message.format(r))


# MDS relation only exists to exchange the MDS endpoint data
# Therefore, there is no need for certificate-related logic
class KafkaMDSRelation(KafkaRelationBase):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8090, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode)
        self._charm = charm
        self._unit = charm.unit
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(
            self._relation_name)
        self._hostname = hostname
        self._port = port
        self._protocol = protocol
        self.state.set_default(mds_list="")
        self.state.set_default(mds_url="")
        self._rbac = rbac_enabled

    @property
    def rbac_enabled(self):
        return self._rbac

    @rbac_enabled.setter
    def rbac_enabled(self, r):
        self._rbac = r
        for r in self.relations:
            if "rbac" in r.data[self.unit]:
                # Avoid resetting it if the value is the same
                if self._rbac == r.data[self.unit]["rbac"]:
                    continue
                r.data[self.unit]["rbac"] = r

    @property
    def mds_url(self):
        return self.state.mds_url

    @mds_url.setter
    def mds_url(self, u):
        self.state.mds_url = u

    @property
    def hostname(self):
        return self._hostname

    @property
    def port(self):
        return self._port

    @property
    def protocol(self):
        return self._protocol

    @hostname.setter
    def hostname(self, x):
        self._hostname = x

    @port.setter
    def port(self, x):
        self._port = x

    @protocol.setter
    def protocol(self, x):
        self._protocol = x

    def get_mds_server_list(self):
        return self.state.mds_list

    def on_mds_relation_joined(self, event):
        pass

    def on_mds_relation_changed(self, event):
        pass

    def _check_rbac_enabled(self):
        for r in self.relations:
            for u in r.units:
                if "rbac" in r.data[u]:
                    # Some units have set RBAC, check if this
                    # unit has also set it
                    if "rbac" not in r.data[self.unit]:
                        raise KafkaMDSRelationRBACNotSetError(r)

    def get_bootstrap_servers(self):
        urls = []
        for r in self.relations:
            for u in r.units:
                if "mds_url" in r.data[u]:
                    urls.append(r.data[u]["mds_url"])
        return ",".join(urls)

    def get_public_key(self, keypath):
        key = ""
        for r in self.relations:
            # Key is set on the other app's relation
            if "public-key" in r.data[r.app]:
                key = r.data[r.app]["public-key"]
                break
        with open(keypath, "w") as f:
            f.write(key)
            f.close()


class KafkaMDSProvidesRelation(KafkaMDSRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8090, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode,
                         hostname, port, protocol, rbac_enabled)
        self.state.set_default(mds_super_user_pwd="")
        self.state.set_default(mds_super_user="")

    @property
    def mds_super_user(self):
        return self.state.mds_super_user

    @mds_super_user.setter
    def mds_super_user(self, u):
        self.state.mds_super_user = u
        if not self.unit.is_leader():
            return
        for r in self.relations:
            if u != r.get(self.model.app, ""):
                r.data[self.model.app]["mds_super_user"] = u

    @property
    def mds_super_user_password(self):
        return self.state.mds_super_user_pwd

    @mds_super_user_password.setter
    def mds_super_user_password(self, p):
        self.state.super_user_pwd = p
        if not self.unit.is_leader():
            return
        for r in self.relations:
            if p != r.get(self.model.app, ""):
                r.data[self.model.app]["mds_super_user_pwd"] = p

    @property
    def mds_url(self):
        return self.state.mds_url

    @mds_url.setter
    def mds_url(self, u):
        """mds_url should be set for both app-level and per unit.
        The reason is that the RBAC settings should target one single
        URL whilst the get_bootstrap_servers() logic in the requirer
        side should get a comma-separated list of URLs"""
        if not self.relations:
            return
        if self.unit.is_leader():
            for r in self.relations:
                if u != r.data[self.model.app].get("mds_url", ""):
                    r.data[self.model.app]["mds_url"] = u
        self.state.mds_url = u
        for r in self.relations:
            if u != r.data[self.unit].get("mds_url", ""):
                r.data[self.unit]["mds_url"] = u

    @property
    def public_key(self):
        if not self.relations:
            return None
        for r in self.relations:
            if "public-key" in r.data[r.app]:
                return r.data[r.app]["public-key"]

    @public_key.setter
    def public_key(self, p):
        if not self.relations:
            return
        if not self.unit.is_leader():
            return
        for r in self.relations:
            if p != r.data[self.model.app].get("public-key", ""):
                r.data[self.model.app]["public-key"] = p

    def on_mds_relation_joined(self, event):
        if not self.unit.is_leader():
            return

    def on_mds_relation_changed(self, event):
        hostname = self._hostname if self._hostname \
            else get_hostname(self.binding_addr)
        url = "{}://{}:{}".format(self._protocol,
                                  hostname,
                                  self._port)
        self.mds_url = url
        if not self.unit.is_leader():
            return
        self._check_rbac_enabled()
        if len(self.state.mds_super_user) == 0:
            raise KafkaMDSRelationConfigIncorrectError(
                "mds_super_user")
        if len(self.state.mds_super_user_pwd) == 0:
            raise KafkaMDSRelationConfigIncorrectError(
                "mds_super_user_password")

    def set_public_key(self, k):
        for r in self.relations:
            if k != r.data[r.app].get("public-key", ""):
                r.data[r.app]["public-key"] = k


class KafkaMDSRequiresRelation(KafkaMDSRelation):
    """
    Requirer Side for MDS relation

    This is a base class for all the custom classes which will generate the
    RBAC requests. This class will be responsible for generating the RBAC POST
    request to the MDS server.

    The actual method that renders the RBAC POST requests is an abstract
    method named "render_rbac_request". That should be reimplemented for
    each of the component's requirer class.

    Parameters:
        req_params: a list of jsons which will be used
                    to negotiate RBAC with MDS server
    """

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8090, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode,
                         hostname, port, protocol, rbac_enabled)
        self.state.set_default(req_params="")
        self.state.set_default(super_user_list="")

    def render_rbac_request(self, params):
        pass

    def generate_configs(self, mds_keypath, mds_user, mds_password):
        """
        Returns a dict with the config options given the user and password.

        Configs:
        - public.key.path: set by MDS provider and used to encrypt tokens
        - confluent.metadata.bootstrap.server.urls: metadata servers'
            addresses or URLs

        If LDAP is set for this application, then also use its user/pwd:
        - confluent.metadata.basic.auth.user.info: <username>:<pwd>
        - confluent.metadata.http.auth.credentials.provider: BASIC

        Parameters:
            mds_keypat: public for MDS will be stored in this file
            mds_user: username for LDAP
            mds_password: password for LDAP
        Returns:
            Dict with the options or None if nothing is yet configured
        """
        if not self.relations:
            return
        props = {}
        props["confluent.metadata.bootstrap.server.urls"] = \
            self.get_bootstrap_servers()
        if len(mds_keypath) > 0:
            self.get_public_key(mds_keypath)
            props["public.key.path"] = mds_keypath
        if len(mds_user) > 0 and \
           len(mds_password) > 0:
            props["confluent.metadata.basic.auth.user.info"] = \
                "{}:{}".format(mds_user, mds_password)
            props["confluent.metadata.http."
                  "auth.credentials.provider"] = "BASIC"
        return props if len(props) > 0 else None

    @property
    def mds_url(self):
        for r in self.relations:
            if "url" in r.data[self.model.app]:
                self.state.mds_url = r.data[self.model.app]["url"]
        return self.state.mds_url

    @property
    def super_user_list(self):
        if len(self.state.super_user_list) == 0:
            return []
        return self.state.super_user_list.split(",")

    @super_user_list.setter
    def super_user_list(self, lst):
        """ Comma-separated list of extra SystemAdmins """
        self.state.super_user_list = ",".join(lst)

    @property
    def mds_super_user_password(self):
        if not self.relations:
            return None
        for r in self.relations:
            if "mds_super_user_pwd" in r.data[r.app]:
                return r.data[r.app]["mds_super_user_pwd"]
        return None

    @property
    def mds_super_user(self):
        if not self.relations:
            return None
        for r in self.relations:
            if "mds_super_user" in r.data[r.app]:
                return r.data[r.app]["mds_super_user"]
        return None

    @property
    def req_params(self):
        """
        req_params: getter of the list of dicts containing
                    the parameters for RBAC
        """
        j = json.loads(self.state.req_params)
        j["kafka-cluster-id"] = self._get_cluster_id_via_mds()
        return j

    def get_public_key(self):
        for r in self.relations:
            if "public-key" in r.data[r.app]:
                return r.data[r.app]["public-key"]

    @req_params.setter
    def req_params(self, p):
        """
        req_params: setter for the list of dicts containing
                    the parameters for RBAC

        The format should be:
        {
            "cluster-id": ....
            "group-id": ....
            ...
        }
        """
        self.state.req_params = json.dumps(p)

    def render_add_extra_super_users(self, params, superusers):
        """ This method adds a list of superusers as SystemAdmins """

        JSON = []
        for u in superusers:
            templ = {
                "ENABLED": True,
                "PATH": "/security/1.0/principals/User:{}"
                        "/roles/SystemAdmin".format(u),
                "DATA": {
                    "clusters": {
                        "kafka-cluster": "{}"
                        .format(params["kafka-cluster-id"]),
                        "connect-cluster": "{}"
                        .format(params["group-id"])
                    }
                } # noqa
            }
            JSON.append(dict(templ))
        return JSON

    def _get_cluster_id_via_mds(self):
        if len(self.mds_super_user_password) == 0:
            er = "mds_super_user_passwod"
            raise KafkaMDSRelationValueNeededNotSetError(er)
        if len(self.mds_super_user) == 0:
            er = "mds_super_user"
            raise KafkaMDSRelationValueNeededNotSetError(er)
        if len(self.mds_url) == 0:
            raise KafkaMDSRelationValueNeededNotSetError("mds_url")
        send_request(self.mds_url, "/kafka/v3/clusters", None,
                     self.mds_user_user, self.mds_super_user_password)

    def _post_request(self, path, data):
        if len(self.mds_super_user_password) == 0:
            er = "mds_super_user_passwod"
            raise KafkaMDSRelationValueNeededNotSetError(er)
        if len(self.mds_super_user) == 0:
            er = "mds_super_user"
            raise KafkaMDSRelationValueNeededNotSetError(er)
        if len(self.mds_url) == 0:
            raise KafkaMDSRelationValueNeededNotSetError("mds_url")
        send_request(self.mds_url, path, data,
                     self.mds_user_user, self.mds_super_user_password)

    def on_mds_relation_joined(self, event):
        pass

    def on_mds_relation_changed(self, event):
        """
        Check if RBAC requests have been raised.
        If not, send POST requests and compare with
        the expected response status.
        """

        if not self.unit.is_leader():
            return
        # RBAC request sent already
        if event.relation.data[self.model.app].get(
           "rbac_request_sent", "").lower() == "true":
            return
        for u in self.render_add_extra_super_users(
           self.req_params,
           self.super_user_list):
            if u["ENABLED"]:
                self._post_request(u["PATH"], u["DATA"])
        for r in self.render_rbac_request(self.req_params):
            if r["ENABLED"]:
                self._post_request(r["PATH"], r["DATA"])
        event.relation.data[self.model.app]["rbac_request_sent"] = "true"


class KafkaMDSRequiresRelationMissingJsonRequestParamError(Exception):

    def __init__(self, param):
        super().__init__("Missing parameter for"
                         " HTTP request to the"
                         " Metadata service: {}".format(param))


class KafkaSchemaRegistryMDSRequiresRelation(KafkaMDSRequiresRelation):
    """
    This class implements the Requirer side for MDS, for Schema Registry.

    """

    # These are the index that convert a json request
    # template from the JSON list
    # on render_rbac_request
    # Those macros represent each of the calls to be
    # executed against the MDS cluster
    REGISTER_CONNECT_CLUSTER = 0
    GRANT_LDAP_USER_SECURITY_ADMIN_RBAC = 1
    GRANT_LDAP_USER_RESOURCE_OWNER_ON_TOPICS_AND_GROUPS = 2

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8090, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode,
                         hostname, port, protocol, rbac_enabled)

    def render_rbac_request(self, params):
        """
        Renders the RBAC requests for Kafka Schema Registry component.
        The JSON dict must be sent via POST to the MDS servers.

        Each element of the JSON format is a template request.
        Use one of the indices above to access those elements.

        """
        # Parameters:
        # 1) Connect cluster name, set via config:
        # kafka-sr-cluster-name
        # 2) Kafka cluster id, can be captured by
        # GET to the MDS, see method: _get_cluster_id_via_mds
        # 3) group-id: identifies the given kafka
        # schema registry cluster, captured via group-id config
        # 4) kafka-hosts: list of dicts with the
        # following format: {"host": hostname, "port":
        # kafka-connect-rest-port}
        #    port is identified on clientPort config
        # 5) kafkastore-topic: value of
        # kafkastore.topic on schema-registry.properties
        # 6) rest-advertised-protocol: value of
        # rest.advertised.protocol on connect.distributed.properties
        # 7) confluent-license-topic: can be retrieved
        # from confluent_license_topic config
        # 8) kafka_sr_ldap_user: LDAP user for Kafka

        # Always override this value:
        params["kafka-cluster-id"] = self._get_cluster_id_via_mds()
        must_have_parameters = ["kafka_sr_cluster_name",
                                "kafka-cluster-id", "group-id",
                                "kafka-hosts", "kafkastore-topic",
                                "rest-advertised-protocol",
                                "confluent-license-topic",
                                "kafka_sr_ldap_user"]
        for el in must_have_parameters:
            if el not in params:
                raise KafkaMDSRequiresRelationMissingJsonRequestParamError(el)
        # 1st JSON: Register Kafka Connect cluster
        # 2nd JSON: Set RBAC permissions for user
        JSON = [
            {
                "ENABLED": True,
                "PATH": "/security/1.0/registry/clusters",
                "DATA": [
                    {
                        "clusterName": "{}"
                        .format(params.get("kafka_sr_cluster_name", "")),
                        "scope": {
                            "clusters": {
                                "kafka-cluster": "{}"
                                .format(params.get("kafka-cluster-id", "")),
                                "schema-registry-cluster": "{}"
                                .format(params.get("group-id", ""))
                            }
                        },
                        "hosts": params.get("kafka-hosts", ""),
                        "protocol": "{}"
                        .format(params.get("rest-advertised-protocol", ""))
                    }
                ]
            },
            {
                "ENABLED": True,
                "PATH": "/security/1.0/principals/User:{}/roles/SecurityAdmin"
                .format(params.get("kafka_sr_ldap_user", "")),
                "DATA": {
                    "clusters": {
                        "kafka-cluster": "{}"
                        .format(params.get("kafka-cluster-id", "")),
                        "schema-registry-cluster": "{}"
                        .format(params.get("group-id", ""))
                    }
                }
            },
            {
                "ENABLED": True,
                "PATH": "/security/1.0/principals/User:{}"
                        "/roles/ResourceOwner/bindings"
                        .format(params.get("kafka_sr_ldap_user", "")),
                "DATA": {
                    "scope": {
                        "clusters": {
                            "kafka-cluster": "{}".format(
                                params.get("kafka-cluster-id", ""))
                            },
                        "resourcePatterns": [
                            {
                                "resourceType": "Group",
                                "name": "{}".format(
                                    params.get("group-id", "")),
                                "patternType": "LITERAL"
                            },
                            {
                                "resourceType": "Topic",
                                "name": "{}".format(
                                    params.get("kafkastore-topic", "")),
                                "patternType": "LITERAL"
                            },
                            {
                                "resourceType": "Topic",
                                "name": "{}".format(
                                    params.get("confluent-license-topic", "")),
                                "patternType": "LITERAL"
                            }
                        ]
                    }
                } # noqa
            },
        ]
        return JSON


class KafkaConnectMDSRequiresRelation(KafkaMDSRequiresRelation):
    """
    This class implements the Requirer side for MDS on Kafka.

    """

    # These are the index that convert a json request
    # template from the JSON list
    # on render_rbac_request
    # Those macros represent each of the calls to be
    # executed against the MDS cluster
    REGISTER_CONNECT_CLUSTER = 0
    GRANT_LDAP_USER_SECURITY_ADMIN_RBAC = 1
    GRANT_LDAP_USER_RESOURCE_OWNER_ON_TOPICS_AND_GROUPS = 2
    GRANT_LDAP_USER_RESOURCE_OWNER_ON_SECRET_REGISTRY = 3
    GRANT_LDAP_USER_RESOURCE_OWNER_ON_INTERCEPTOR = 4

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=8090, protocol="https",
                 rbac_enabled=False):
        super().__init__(charm, relation_name, user, group, mode,
                         hostname, port, protocol, rbac_enabled)
        self.state.set_default(kafka_connect_secret_enabled="")
        self.state.set_default(kafka_connect_telemetry_enabled="")

    @property
    def kafka_connect_secret_enabled(self):
        if len(self.state.kafka_connect_secret_enabled) == 0 or \
           self.state.kafka_connect_secret_enabled.lower() == "false":
            return False
        return True

    @kafka_connect_secret_enabled.setter
    def kafka_connect_secret_enabled(self, x):
        self.state.kafka_connect_secret_enabled = x

    @property
    def kafka_connect_telemetry_enabled(self):
        if len(self.state.kafka_connect_telemetry_enabled) == 0 or \
           self.state.kafka_connect_telemetry_enabled.lower() == "false":
            return False
        return True

    @kafka_connect_telemetry_enabled.setter
    def kafka_connect_telemetry_enabled(self, x):
        self.state.kafka_connect_telemetry_enabled = x

    def render_rbac_request(self, params):
        """
        Renders the RBAC requests for Kafka Connect component.
        The JSON dict must be sent via POST to the MDS servers.

        Each element of the JSON format is a template request.
        Use one of the indices above to access those elements.

        """
        # Parameters:
        # 1) Connect cluster name, set via config:
        # kafka-connect-cluster-name
        # 2) Kafka cluster id, can be captured by
        # GET to the MDS, see method: _get_cluster_id_via_mds
        # 3) group-id: identifies the given kafka
        # connect cluster, captured via group-id config
        # 4) kafka-hosts: list of dicts with the
        # following format: {"host": hostname, "port":
        # kafka-connect-rest-port}
        #    port is identified on clientPort config
        # 5) config-storage-topic: value of
        # config.storage.topic on connect-distributed.properties
        # 6) rest-advertised-protocol: value of
        # rest.advertised.protocol on connect.distributed.properties
        # 7) confluent-license-topic: can be retrieved
        # from confluent_license_topic config
        # 8) kafka_connect_ldap_user: LDAP user for Kafka

        # Always override this value:
        params["kafka-cluster-id"] = self._get_cluster_id_via_mds()
        must_have_parameters = ["kafka_connect_cluster_name",
                                "kafka-cluster-id", "group-id",
                                "kafka-hosts", "config-storage-topic",
                                "rest-advertised-protocol",
                                "offset-storage-topic", "status-storage-topic",
                                "confluent-license-topic",
                                "kafka_connect_ldap_user"]
        for el in must_have_parameters:
            if el not in params:
                raise KafkaMDSRequiresRelationMissingJsonRequestParamError(el)
        # 1st JSON: Register Kafka Connect cluster
        # 2nd JSON: Set RBAC permissions for user
        JSON = [
            {
                "ENABLED": True,
                "PATH": "/security/1.0/registry/clusters",
                "DATA": [
                    {
                        "clusterName": "{}"
                        .format(params.get("kafka_connect_cluster_name", "")),
                        "scope": {
                            "clusters": {
                                "kafka-cluster": "{}"
                                .format(params.get("kafka-cluster-id", "")),
                                "connect-cluster": "{}"
                                .format(params.get("group-id", ""))
                            }
                        },
                        "hosts": params.get("kafka-hosts", ""),
                        "protocol": "{}"
                        .format(params.get("rest-advertised-protocol", ""))
                    }
                ]
            },
            {
                "ENABLED": True,
                "PATH": "/security/1.0/principals/User:{}/roles/SecurityAdmin"
                .format(params.get("kafka_connect_ldap_user", "")),
                "DATA": {
                    "clusters": {
                        "kafka-cluster": "{}"
                        .format(params.get("kafka-cluster-id", "")),
                        "connect-cluster": "{}".format(
                            params.get("group-id", ""))
                    }
                }
            },
            {
                "ENABLED": True,
                "PATH": "/security/1.0/principals/User:{}"
                        "/roles/ResourceOwner/bindings"
                        .format(params.get("kafka_connect_ldap_user", "")),
                "DATA": {
                    "scope": {
                        "clusters": {
                            "kafka-cluster": "{}".format(
                                params.get("kafka-cluster-id", ""))
                            },
                        "resourcePatterns": [
                            {
                                "resourceType": "Group",
                                "name": "{}".format(
                                    params.get("group-id", "")),
                                "patternType": "LITERAL"
                            },
                            {
                                "resourceType": "Topic",
                                "name": "{}".format(
                                    params.get("config-storage-topic", "")),
                                "patternType": "LITERAL"
                            },
                            {
                                "resourceType": "Topic",
                                "name": "{}".format(
                                    params.get("offset-storage-topic", "")),
                                "patternType": "LITERAL"
                            },
                            {
                                "resourceType": "Topic",
                                "name": "{}".format(
                                    params.get("status-storage-topic", "")),
                                "patternType": "LITERAL"
                            },
                            {
                                "resourceType": "Topic",
                                "name": "{}".format(
                                    params.get("confluent-license-topic", "")),
                                "patternType": "LITERAL"
                            }
                        ]
                    }
                } # noqa
            },
            {
                "ENABLED": self.kafka_connect_secret_enabled,
                "PATH": "/security/1.0/principals/User:{}"
                        "/roles/ResourceOwner/bindings".format(
                            params.get("kafka_connect_ldap_user", "")),
                "DATA": {
                    "scope": {
                        "clusters": {
                            "kafka-cluster": "{}".format(
                                params.get("kafka-cluster-id", ""))
                            },
                        "resourcePatterns": [
                            {
                                "resourceType": "Group",
                                "name": "{}".format(
                                    params.get("config.providers.secret.param"
                                               ".secret.registry.group.id",
                                               "")),
                                "patternType": "LITERAL"
                            },
                            {
                                "resourceType": "Topic",
                                "name": "{}".format(params.get(
                                    "config.providers.secret"
                                    ".param.kafkastore.topic", "")),
                                "patternType": "LITERAL"
                            }
                        ]
                    }
                } # noqa
            },
            {
                "ENABLED": self.kafka_connect_telemetry_enabled,
                "PATH": "/security/1.0/principals/User:{}"
                        "/roles/DeveloperWrite/bindings".format(
                            params.get("kafka_connect_ldap_user", "")),
                "DATA": {
                    "scope": {
                        "clusters": {
                            "kafka-cluster": "{}".format(
                                params.get("kafka-cluster-id", ""))
                        },
                        "resourcePatterns": [{
                            "resourceType": "Topic",
                            "name": "{}".format(
                                params.get('confluent.monitoring'
                                           '.interceptor.topic', "")),
                            "patternType": "LITERAL"
                        }]
                    }
                } # noqa
            }
        ]
        return JSON
