"""

Implements the Listener negotiation


The Requirer side of the relation submits a request for a listener
following a given pattern. That request is implemented by the provider side
(kafka broker).

The kafka broker side will read each of the requests and implement as a
different listener. It will also add metadata specific to the given listener
e.g. passwords for trust and keystores. Check method: _get_default_listeners
for an example.

Eventually, these listeners are passed to a method that implements
the configs back to the brokers.

Separating the listeners into a negotiation protocol across the relations
allows different listeners to be configured on the end-charm and the
information is sent via relation to the listener to just implement it.


Parameters of the request:

endpoint and advertise)

Each broker receives the request and implements it with its own IP/hostname
information. Therefore, endpoints should be marked as *BINDING* or *ADVERTISE*
Those strings will be replaced with the corresponding IP or hostname of the
binding_address or advertise_address for the listener space.

is_public)

This attribute selects if the advertise or the binding IP will be used to
set this listener

plaintext_pwd)

Optional parameter that allows to set a password authentication using plaintext
communications. If this is empty, then no password is set for authentication.

secprot)

One of the values is possible: PLAINTEXT, SSL, SASL, SASL_SSL.

SASL)

This subdictionary describes the authentication mechanisms available.
If confluent is used, check:
https://docs.confluent.io/platform/current/kafka/ \
    authentication_sasl/index.html
It allows to exchange details of each authentication mechanism.
SASL dict has some fields that should be implemented for any auth method:
FOR NOW, only OAUTHBEARER and GSSAPI are enabled.

SASL.protocol)

Informs which SASL protocol will be used.

cert)

Actual certificate chain to be used by the client in the mTLS authentication.
FOR NOW, MTLS IS NOT AVAILABLE.



Examples of requests:
# 1) Request for a basic listener:
Request a listener with no certificates (PLAINTEXT) or auth (SASL)

{
    "is_public": False,
    "plaintext_pwd": "",
    "secprot": "PLAINTEXT",
    "cert": ""
}

# 2) Request for a listener with server-side SSL and exposed on the
advertise_address:

{
    "is_public": True,
    "plaintext_pwd": "",
    "secprot": "SSL",
    "cert": ""
}

# 3) Request SASL_SSL with GSSAPI:

{
    "is_public": True,
    "plaintext_pwd": "",
    "secprot": "SASL_SSL",
    "SASL": {
        "protocol": "GSSAPI",
        "kerberos-principal": <principal-string>,
        "kerberos-protocol": <protocol>
    },
    "cert": ""
}

# 4) Request for SASL_SSL with OAUTHBEARER on Confluent platform

{
    "is_public": True,
    "plaintext_pwd": "",
    "secprot": "SASL_SSL",
    "SASL": {
        "protocol": "oauthbearer",
        "publicKeyPath": <file-path>,
        "publicKey": base64-public-key,
        "confluent": {
            "login.callback": callback,
            "server.callback": callback
        }
    },
    "cert": ""
}

"""

import json
import copy
import logging

from charms.kafka_broker.v0.kafka_security import genRandomPassword
from charms.kafka_broker.v0.kafka_relation_base import KafkaRelationBase
from charms.kafka_broker.v0.kafka_linux import get_hostname

logger = logging.getLogger(__name__)

__all__ = [
    "KafkaListenerRelation",
    "KafkaListenerProvidesRelation",
    "KafkaListenerRequiresRelation",
    "KafkaListenerRelationNotSetError",
    "KafkaListenerRelationEmptyListenerDictError"
]


class KafkaListenerRelationEmptyListenerDictError(Exception):
    def __init__(self,
                 message="Empty Listener dict provided"):
        super().__init__(message)


class KafkaListenerRelationNotSetError(Exception):
    def __init__(self,
                 message="Relation not ready, waiting for connection"):
        super().__init__(message)


class KafkaListenerRelation(KafkaRelationBase):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=443, protocol="SSL"):
        super().__init__(charm, relation_name, user, group, mode)
        self._hostname = hostname
        self._port = port
        self._protocol = protocol

    @property
    def hostname(self):
        return self._hostname if self._hostname \
            else get_hostname(self.binding_addr)

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

    def on_listener_relation_joined(self, event):
        pass

    def on_listener_relation_changed(self, event):
        pass


# Provider side is run by kafka broker: it publishes its endpoint for a given
# client to use it. On the other side, it receives a dict describing what the
# other application waits as listener (SASL or not, e.g. kerberos) or SSL.
class KafkaListenerProvidesRelation(KafkaListenerRelation):

    # Ports 9092-9094 reserved for internal, broker and external
    def __init__(self, charm, relation_name,
                 user="", group="", mode=0,
                 hostname=None, port=9092, protocol="SSL"):
        super().__init__(charm, relation_name, user, group, mode,
                         hostname, port, protocol)
        self.state.set_default(available_port=port)
        self.state.set_default(internal_pwd=genRandomPassword(12))
        self.state.set_default(external_pwd=genRandomPassword(12))
        self.state.set_default(broker_pwd=genRandomPassword(12))
        self.listeners = {}

    @property
    def internal_pwd(self):
        return self.state.internal_pwd

    @internal_pwd.setter
    def internal_pwd(self, p):
        self.state.internal_pwd = p

    @property
    def external_pwd(self):
        return self.state.external_pwd

    @external_pwd.setter
    def external_pwd(self, p):
        self.state.external_pwd = p

    @property
    def broker_pwd(self):
        return self.state.broker_pwd

    @broker_pwd.setter
    def broker_pwd(self, p):
        self.state.broker_pwd = p

    @property
    def available_port(self):
        return int(self.state.available_port)

    @available_port.setter
    def available_port(self, p):
        # We should expect the p value to be either a string
        # or an int, therefore, force typecast
        self.state.available_port = str(p)

    def _get_default_listeners(self, keystore_path, keystore_pwd, clientauth):
        listeners = {
            "internal": {
                "endpoint": "INTERNAL://*BINDING*:{}".format(
                    self.available_port),
                "advertise": "INTERNAL://*BINDING*:{}".format(
                    self.available_port),
                "is_public": False,
                "plaintext_pwd": self.internal_pwd,
                "cert_present": True,
                "sasl_present": False,
                "secprot": "SSL",
                "port": self.available_port,
                "SASL": {},
                "cert": "",
                "ts_path": self.ts_path,
                "ts_pwd": self.ts_pwd,
                "ks_path": keystore_path,
                "ks_pwd": keystore_pwd
            },
            "external": {
                "endpoint": "EXTERNAL://*ADVERTISE*:{}".format(
                    self.available_port + 1),
                "advertise": "EXTERNAL://*ADVERTISE*:{}".format(
                    self.available_port + 1),
                "is_public": True,
                "plaintext_pwd": self.external_pwd,
                "cert_present": True,
                "sasl_present": False,
                "secprot": "SSL",
                "port": self.available_port + 1,
                "SASL": {},
                "cert": "",
                "ts_path": self.ts_path,
                "ts_pwd": self.ts_pwd,
                "ks_path": keystore_path,
                "ks_pwd": keystore_pwd
            },
            "broker": {
                "endpoint": "BROKER://*BINDING*:{}".format(
                    self.available_port + 2),
                "advertise": "BROKER://*BINDING*:{}".format(
                    self.available_port + 2),
                "is_public": False,
                "plaintext_pwd": self.broker_pwd,
                "cert_present": True,
                "sasl_present": False,
                "secprot": "SSL",
                "port": self.available_port + 2,
                "SASL": {},
                "cert": "",
                "ts_path": self.ts_path,
                "ts_pwd": self.ts_pwd,
                "ks_path": keystore_path,
                "ks_pwd": keystore_pwd
            }
        }
        return listeners

    # Generates a string with the template dict
    # That template dict contains entries that should be replaced
    # on each node: *BINDING* and *ADVERTISE* for its respective
    # binding and advertise addresses or hostnames.
    def get_unit_listener(self,
                          keystore_path,
                          keystore_pwd,
                          get_default=True,
                          clientauth=False,
                          bootstrap_url=None):
        """Get the listener requests from the relations.

        Iterate over each relation and capture the requests for endpoints.
        Optionally, a custom URL for bootstrap can be specified instead of
        using the available IP + PORT.
        """
        self.available_port = self.port
        if not self.unit.is_leader():
            return {}
        if not self.relations:
            # No relations available, return just the defaults
            # if defaults are True, otherwise return empty {}
            return json.dumps(self._get_default_listeners(
                keystore_path, keystore_pwd, clientauth)
                    if get_default else {})

        listeners = {}
        # Leader sets the value
        if get_default:
            listeners = self._get_default_listeners(
                keystore_path, keystore_pwd, clientauth)
        # Consider the 3 ports to be used if default is enabled
        self.available_port += 3
        for r in self.relations:
            if "request" in r.data[r.app]:
                inter = r.data[r.app]["request"]
            else:
                continue
            # In some situations, inter is coming with between ""
            if inter[0] == "\"":
                inter = inter[1:]
            if inter[-1] == "\"":
                inter = inter[:-1]
            req = json.loads(inter.replace("\\\"", "\""))
            if not req:
                # for the case req = {}
                continue
            # Jump to the next port available
            self.available_port += 1
            listener_name = r.app.name.replace("-", "_")
            addr = None
            if req.get("is_public", False):
                addr = "*ADVERTISE*"
            else:
                addr = "*BINDING*"
            if not bootstrap_url:
                lt = listener_name + "://" + addr + ":" + \
                    str(self.available_port)
            else:
                lt = listener_name + "://" + bootstrap_url + ":" + \
                    str(self.available_port)
            listeners[listener_name] = {}
            listeners[listener_name]["bootstrap_server"] = \
                addr + ":" + \
                str(self.available_port)
            # Store ports because we will need for open_port and close_port
            listeners[listener_name]["port"] = self.available_port
            listeners[listener_name]["endpoint"] = lt
            listeners[listener_name]["advertise"] = lt
            listeners[listener_name]["secprot"] = req["secprot"]
            listeners[listener_name]["SASL"] = \
                copy.deepcopy(req.get("SASL", {}))
            listeners[listener_name]["plaintext_pwd"] = \
                req["plaintext_pwd"]
            listeners[listener_name]["cert_present"] = \
                len(req.get("cert", "")) > 0
            listeners[listener_name]["sasl_present"] = \
                "SASL" in req
            listeners[listener_name]["ts_path"] = self.ts_path
            listeners[listener_name]["ts_pwd"] = self.ts_pwd
            listeners[listener_name]["ks_path"] = keystore_path
            listeners[listener_name]["ks_pwd"] = keystore_pwd
            listeners[listener_name]["clientauth"] = clientauth
            # Resolve SASL for the request:
            if listeners[listener_name]["sasl_present"]:
                prot = ""
                if "protocol" in listeners[listener_name]["SASL"]:
                    prot = listeners[listener_name]["SASL"]["protocol"]
                if prot == "OAUTHBEARER" and \
                   "mds-info" in self.relation.data[self.charm.app]:
                    # mds info has been configured, load it
                    sasl = listeners[listener_name]["SASL"]
                    mds_info = self.relation.data[self.charm.app]["mds-info"]
                    jaas_config = \
                        "org.apache.kafka.common.security.oauthbearer." + \
                        "OAuthBearerLoginModule required username=\"{}\"" + \
                        " password=\"{}\" metadataServerUrls=\"{}\";"
                    sasl["jaas.config"] = jaas_config.format(
                        mds_info["username"], mds_info["password"],
                        mds_info["mds_url"])
                    sasl["confluent"]["login.callback"] = \
                        mds_info["login.callback"]
                    sasl["confluent"]["server.callback"] = \
                        mds_info["server.callback"]

        # update all the units
        self.listeners = copy.deepcopy(listeners)
        return json.dumps(listeners)

    def set_mds_enpoint(self, mds_url, mds_username, mds_password):
        if not self.relations or not self.unit.is_leader():
            return
        for r in self.relations:
            r.data[self.charm.app]["mds-info"] = json.dumps({
                "mds_url": mds_url,
                "username": mds_username,
                "password": mds_password,
                "login.callback": \
                    "io.confluent.kafka.server.plugins.auth.token.TokenBearerServerLoginCallbackHandler", # noqa
                "server.callback": \
                    "io.confluent.kafka.server.plugins.auth.token.TokenBearerValidatorCallbackHandler" # noqa
            })

    def get_sasl_mechanisms_list(self):
        result = set()
        for k, v in self.listeners.items():
            if "protocol" in v["SASL"]:
                result.add(v["SASL"]["protocol"])
        return result

    def _convert_listener_template(self, lst):
        if not lst or len(lst) == 0:
            raise KafkaListenerRelationEmptyListenerDictError()
        listeners = lst.replace(
            "*BINDING*", get_hostname(self.binding_addr))
        listeners = listeners.replace(
            "*ADVERTISE*", get_hostname(self.advertise_addr))
        listeners = json.loads(listeners)
        return listeners

    def _generate_opts(self, _lst,
                       keystore_path,
                       keystore_pwd,
                       publicKeyPath,
                       get_default=True,
                       clientauth=False):
        if not _lst:
            raise KafkaListenerRelationEmptyListenerDictError()
        # In case _lst comes as None
        lst = _lst or "{}"
        # Convert the template
        listeners = self._convert_listener_template(lst)
        # Now set the options
        listener_opts = {}
        listener_opts["listeners"] = ",".join(
            [l["endpoint"]
             for k, l in listeners.items()])
        listener_opts["advertised.listeners"] = ",".join(
            [l["advertise"]
             for k, l in listeners.items()])
        listener_opts["listener.security.protocol.map"] = ",".join(
            ["{}:{}".format(k, l["secprot"]) for k, l in listeners.items()])
        prefix = "listener.name."
        for k, v in listeners.items():
            if v["sasl_present"]:
                listener_opts[prefix + k +
                              ".sasl.enabled.mechanisms"] = \
                    v["SASL"]["protocol"]
                if v["SASL"]["protocol"] == "GSSAPI":
                    listener_opts[prefix + k +
                                  ".gssapi.sasl.jaas.config"] = \
                        'com.sun.security.auth.module.Krb5LoginModule ' + \
                        'required useKeyTab=true storeKey=true keyTab=' + \
                        '"/etc/security/keytabs/kafka_broker.keytab" ' + \
                        'principal="{}";'.format(
                            v["SASL"]["kerberos-principal"])
                    listener_opts[prefix + k +
                                  ".sasl.kerberos.service.name"] = \
                        v["SASL"]["kerberos-protocol"]
                if v["SASL"]["protocol"] == "OAUTHBEARER":
                    listener_opts[prefix + k +
                                  ".sasl.jaas.config"] = \
                        'org.apache.kafka.common.security.oauthbearer.' + \
                        'OAuthBearerLoginModule required'
                    listener_opts[prefix + k +
                                  ".sasl.jaas.config"] += \
                        ' publicKeyPath="{}";'.format(publicKeyPath)
                    if "confluent" in v["SASL"]:
                        if "login.callback" in v["SASL"]["confluent"]:
                            listener_opts[prefix + k +
                                          ".oauthbearer.sasl.login.callback"
                                          ".handler.class"] = \
                                v["SASL"]["confluent"]["login.callback"]
                        if "server.callback" in v["SASL"]["confluent"]:
                            listener_opts[prefix + k +
                                          ".oauthbearer.sasl.server.callback"
                                          ".handler.class"] = \
                                v["SASL"]["confluent"]["server.callback"]
            if v["cert_present"]:
                listener_opts[prefix +
                              k + ".ssl.client.auth"] = \
                                  "required" if clientauth else "none"
                listener_opts[prefix +
                              k + ".ssl.key.password"] = keystore_pwd
                listener_opts[prefix +
                              k + ".ssl.keystore.location"] = keystore_path
                listener_opts[prefix +
                              k + ".ssl.keystore.password"] = keystore_pwd
                if len(self.ts_path) > 0:
                    listener_opts[prefix + k + ".ssl.truststore.location"] = \
                        self.ts_path
                    listener_opts[prefix + k + ".ssl.truststore.password"] = \
                        self.ts_pwd
        return listener_opts

    def on_listener_relation_joined(self, event):
        # There is nothing to do at first from kafka broker perspective.
        # Each charm will publish at -joined event what it is looking for:
        # PLAINTEXT, SSL, SASL_SSL, etc and if this is a public or
        # private link.
        return

    def on_listener_relation_changed(self, event):
        # Check certificates across this unit and remotes. That avoids errors
        # down the road if some apps are using SSL set and others not.
        if not self.is_TLS_enabled():
            return
        self._get_all_tls_cert()

    def set_bootstrap_data(self, lst):
        if not lst or len(lst) == 0:
            raise KafkaListenerRelationEmptyListenerDictError()
        if not self.relations:
            # We may work with defaults only and no charm related to it
            return
        for r in self.relations:
            listener = self._convert_listener_template(lst)
            for k, v in listener.items():
                data = {}
                # Make a copy, so we can change its content
                data[k] = copy.deepcopy(listener[k])
                if "ts_pwd" in data[k]:
                    del data[k]["ts_pwd"]
                if "ks_pwd" in data[k]:
                    del data[k]["ks_pwd"]
            logger.debug("Listeners: set_bootstrap_data={}".format(listener))
            j = json.dumps(listener)
            if j != r.data[self.unit].get("bootstrap-data", ""):
                r.data[self.unit]["bootstrap-data"] = j


# Requirer is run on the charm clients connecting to kafka brokers
class KafkaListenerRequiresRelation(KafkaListenerRelation):

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name, user, group, mode)
        self.state.set_default(is_public=False)
        self.state.set_default(request="{}")
        if len(json.loads(self.request).get("plaintext_pwd", "")) > 0:
            # Either password is empty or there is no password set.
            # Set it in this case. Otherwise it will change every run.
            self.set_plaintext_pwd(genRandomPassword())

    @property
    def request(self):
        return copy.deepcopy(self.state.request)

    @request.setter
    def request(self, r):
        self.state.request = r

    def set_plaintext_pwd(self, pwd):
        req = json.loads(self.request) or {}
        # changing relation data will trigger a -changed event on
        # the Provides side, which also triggers a relation data update.
        # That can lead to an infinite loop of changes.
        if "plaintext_pwd" in req:
            if pwd == req["plaintext_pwd"]:
                return
        req["plaintext_pwd"] = pwd
        self.request = json.dumps(req)
        self.set_request(req)

    def set_sasl(self, sasl):
        req = json.loads(self.request) or {}
        # changing relation data will trigger a -changed event on
        # the Provides side, which also triggers a relation data update.
        # That can lead to an infinite loop of changes.
        if "SASL" in req:
            if sasl == req["SASL"]:
                return
        req["SASL"] = sasl
        self.request = json.dumps(req)
        self.set_request(req)

    def set_is_public(self, is_public):
        req = json.loads(self.request) or {}
        # changing relation data will trigger a -changed event on
        # the Provides side, which also triggers a relation data update.
        # That can lead to an infinite loop of changes.
        if "is_public" in req:
            if is_public == req["is_public"]:
                return
        req["is_public"] = is_public
        self.request = json.dumps(req)
        self.set_request(req)

    def tls_client_auth_enabled(self):
        for r in self.relations:
            for u in r.units:
                if "clientauth" in r.data[u]:
                    return r.data[u]["clientauth"]
        return False

    def set_request(self, req):
        j = json.dumps(req)
        # changing relation data will trigger a -changed event on
        # the Provides side, which also triggers a relation data update.
        # That can lead to an infinite loop of changes.
        if j == self.request:
            return
        self.state.request = j
        self._set_request()

    def _set_request(self):
        if not self.relations:
            return
        if not self.charm.unit.is_leader():
            return
        for r in self.relations:
            if isinstance(self.request, str):
                r.data[self.charm.app]["request"] = self.request
            else:
                r.data[self.charm.app]["request"] = json.dumps(self.request)

    def get_bootstrap_servers(self):
        if not self.relations:
            raise KafkaListenerRelationNotSetError()
        # The Requires side published its request on the relation
        # Kafka-broker generated the listener map based on the request
        # Pushes back the available listeners and the Requires side can build
        # its bootstrap-servers option
        servers = []
        lst_name = self.unit.app.name.replace("-", "_")
        for r in self.relations:
            for u in r.units:
                try:
                    req = json.loads(r.data[u]["bootstrap-data"])
                    endpoint = \
                        req[lst_name]["bootstrap_server"]
                except KeyError:
                    raise KafkaListenerRelationNotSetError()
                servers.append(endpoint)
        return ",".join(servers)

    def get_bootstrap_data(self):
        if not self.relations:
            raise KafkaListenerRelationNotSetError()
        lst_name = self.unit.app.name.replace("-", "_")
        for r in self.relations:
            for u in r.units:
                if "bootstrap-data" in r.data[u]:
                    j = json.loads(r.data[u]["bootstrap-data"])
                    return copy.deepcopy(j[lst_name])

    def get_mds_enpoint(self):
        if not self.relations:
            return
        mds_info = []
        for r in self.relations:
            for u in r.units:
                if "mds-info" in r.data[u]:
                    mds_info.append(json.loads(r.data[u]["mds-info"]))
        return mds_info

    def set_TLS_auth(self,
                     cert_chain,
                     truststore_path,
                     truststore_pwd,
                     user=None,
                     group=None,
                     mode=None):
        req = json.loads(self.request) or {}
        req["cert"] = cert_chain
        self.set_request(req)
        super().set_TLS_auth(cert_chain, truststore_path,
                             truststore_pwd, user, group, mode)

    def on_listener_relation_joined(self, event):
        self._get_all_tls_cert()

    def on_listener_relation_changed(self, event):
        self.on_listener_relation_joined(event)

    def generate_options(self,
                         keystore_path,
                         keystore_pwd,
                         truststore_path,
                         truststore_pwd,
                         prefix="",
                         get_listener=False,
                         clientauth=False):
        """Generates the options based on the listener provided
        by the broker charm. """
        result = {}
        result[prefix + "bootstrap.servers"] = self.get_bootstrap_servers()
        v = self.get_bootstrap_data()
        # If get bootstrap data is not yet available, return empty
        if not v:
            return None
        result[prefix + "security.protocol"] = v["secprot"]

        if len(keystore_path) > 0 and v["cert_present"]:
            result[prefix + "ssl.key.password"] = keystore_pwd
            result[prefix + "ssl.keystore.location"] = keystore_path
            result[prefix + "ssl.keystore.password"] = keystore_pwd
            if clientauth:
                result[prefix + "ssl.client.auth"] = "required"
        if len(truststore_path) > 0 and v["cert_present"]:
            result[prefix + "ssl.truststore.location"] = truststore_path
            result[prefix + "ssl.truststore.password"] = truststore_pwd

        if v["sasl_present"]:
            sasl = v["SASL"]
            result[prefix + "sasl.mechanism"] = v["SASL"]["protocol"]
            if v["SASL"]["protocol"] == "OAUTHBEARER":
                result[prefix + "sasl.jaas.config"] = sasl["jaas.config"]
                if "confluent" in sasl:
                    if "login.callback" in sasl["confluent"]:
                        result[prefix +
                               "sasl.login.callback.handler.class"] = \
                            sasl["confluent"]["login.callback"]
                    if "server.callback" in sasl["confluent"]:
                        result[prefix +
                               "sasl.server.callback"
                               ".handler.class"] = \
                            sasl["confluent"]["server.callback"]
            elif v["SASL"]["protocol"] == "GSSAPI":
                result[prefix +
                       "sasl.jaas.config"] = \
                    'com.sun.security.auth.module.Krb5LoginModule ' + \
                    'required useKeyTab=true storeKey=true keyTab=' + \
                    '"/etc/security/keytabs/kafka_broker.keytab" ' + \
                    'principal="{}";'.format(
                        sasl["kerberos-principal"])
                result[prefix +
                       "sasl.kerberos.service.name"] = \
                    sasl["kerberos-protocol"]

        return result if len(result) > 0 else None
