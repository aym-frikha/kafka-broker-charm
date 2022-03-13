from ops.framework import Object
from ops.framework import StoredState

from charms.kafka_broker.v0.kafka_security import CreateTruststore
from charms.kafka_broker.v0.kafka_linux import get_hostname

__all__ = [
    "KafkaRelationBaseNotUsedError",
    "KafkaRelationBaseTLSNotSetError",
    "KafkaRelationBase",
]


class KafkaRelationBaseTLSNotSetError(Exception):

    def __init__(self,
                 message="TLS detected on the relation but not on this unit"):
        super().__init__(message)


class KafkaRelationBaseNotUsedError(Exception):
    def __init__(self,
                 message="There is no connections to this relation yet"):
        super().__init__(message)


class KafkaRelationBase(Object):

    state = StoredState()

    def __init__(self, charm, relation_name,
                 user="", group="", mode=0):
        super().__init__(charm, relation_name)

        self._charm = charm
        self._unit = charm.unit
        self._relation_name = relation_name
        # Keeping _relation for compatibility reasons
        self._relation = self.relation
        # :: separated list of trusted_certs for TLS
        self.state.set_default(trusted_certs="")
        self.state.set_default(ts_path="")
        self.state.set_default(ts_pwd="")
        self.state.set_default(user=user)
        self.state.set_default(group=group)
        self.state.set_default(mode=mode)

    @property
    def ts_path(self):
        return self.state.ts_path

    @ts_path.setter
    def ts_path(self, t):
        self.state.ts_path = t

    @property
    def ts_pwd(self):
        return self.state.ts_pwd

    @ts_pwd.setter
    def ts_pwd(self, t):
        self.state.ts_pwd = t

    @property
    def charm(self):
        return self._charm

    @property
    def user(self):
        return self.state.user

    @user.setter
    def user(self, x):
        if not x or len(x) == 0:
            return
        self.state.user = x

    @property
    def hostname(self):
        return get_hostname(self.binding_addr)

    @property
    def group(self):
        return self.state.group

    @group.setter
    def group(self, x):
        if not x or len(x) == 0:
            return
        self.state.group = x

    @property
    def mode(self):
        return self.state.mode

    @mode.setter
    def mode(self, x):
        self.state.mode = x

    @property
    def unit(self):
        return self._unit

    def all_units(self, relation):
        u = []
        if not relation:
            return u
        if isinstance(relation, list):
            for r in relation:
                for uni in r.units:
                    u.append(uni)
        else:
            u = relation.units.copy()
        # as .copy may have been used, that can be a set():
        if isinstance(u, set):
            u.add(self.unit)
        if isinstance(u, list):
            u.append(self.unit)
        return u

    @property
    def relation(self):
        # Given that get_relation returns either one, None
        # or throws TooManyRelatedAppsError
        # It is better to always return the first element of relations
        if not self.relations:
            return
        return self.relations[0]

    @property
    def relations(self):
        return self.framework.model.relations[self._relation_name]

    def _get_all_tls_cert(self, ext_list=[], extra_cas=[]):
        """Captures all TLS information from relations and generates Truststore. """
        if not self.is_TLS_enabled():
            # If there is no tls announced by relation peers,
            # then CreateTruststore is not needed. Do not raise an Exception
            # given it will use non-encrypted communication instead
            return
        crt_list = list(ext_list)
        for r in self.relations:
            for u in self.all_units(r):
                if "tls_cert" in r.data[u]:
                    crt_list.append(r.data[u]["tls_cert"])
        self.state.trusted_certs = \
            "::".join(crt_list)
        CreateTruststore(self.state.ts_path,
                         self.state.ts_pwd,
                         self.state.trusted_certs.split("::"),
                         ts_regenerate=True,
                         user=self.state.user,
                         group=self.state.group,
                         mode=self.state.mode,
                         extra_cas=extra_cas)

    def is_TLS_enabled(self, relation=None):
        if not relation and not self.relations:
            raise KafkaRelationBaseNotUsedError()
        rel = relation
        if not rel:
            rel = self.relations
        elif isinstance(rel, list):
            rel = relation
        else:
            rel = [relation]
        for r in rel:
            for u in self.all_units(r):
                if "tls_cert" in r.data[u]:
                    # It is enabled, now we check
                    # if we have it set this unit as well
                    if "tls_cert" not in r.data[self.unit]:
                        # we do not, so raise an error to inform it
                        raise KafkaRelationBaseTLSNotSetError()
                    return True
        return False

    def set_TLS_auth(self,
                     cert_chain,
                     truststore_path,
                     truststore_pwd,
                     user=None,
                     group=None,
                     mode=None,
                     extra_certs=[],
                     extra_cas=[]):
        """Sets the certificates for all the relation units and creates a
        truststore file to store them.

        To trust each other, units need to trust the same CAs and certs.
        The set_TLS_auth does the following:
        1) Add current certificate passed as cert_chain to the relation;
        2) Adds extra certificates coming, e.g. from other relations that
           are composed of CA chain+certificate using extra_certs;
        3) Adds extra CAs to be trusted, e.g. via trust action.
        """
        if not self.relations:
            raise KafkaRelationBaseNotUsedError()
        for r in self.relations:
            # 1) Publishes the cert on tls_cert
            if cert_chain == r.data[self.unit].get("tls_cert", ""):
                continue
            r.data[self.unit]["tls_cert"] = cert_chain
        self.state.ts_path = truststore_path
        self.state.ts_pwd = truststore_pwd
        self.state.trusted_certs = cert_chain
        if user:
            self.state.user = user
        if group:
            self.state.group = group
        if mode:
            self.state.mode = mode
        # 2) Grab any already-published tls certs and generate the truststore
        self._get_all_tls_cert(ext_list=extra_certs, extra_cas=extra_cas)

    @property
    def peer_addresses(self):
        addresses = []
        for u in self.relation.units:
            addresses.append(str(self.relation.data[u]["ingress-address"]))
        return addresses

    @property
    def advertise_addr(self):
        m = self.model
        return str(m.get_binding(self._relation_name).network.ingress_address)

    @property
    def binding_addr(self):
        m = self.model
        return str(m.get_binding(self._relation_name).network.bind_address)
