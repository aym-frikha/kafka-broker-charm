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

Implements tasks related to Java, such as installing openjdk and generating
Keystores.

JavaCharmBase defines the "ks" StoredState, which can be used by all its
children classes.

"""

import logging

from ops.charm import CharmBase
from ops.framework import StoredState

from charms.kafka_broker.v0.kafka_security import (
    genRandomPassword,
    PKCS12CreateKeystore
)

from charms.kafka_broker.v0.charmhelper import (
    apt_update, apt_install)

logger = logging.getLogger(__name__)


__all__ = [
    'JavaCharmBase'
]


class JavaCharmBase(CharmBase):

    PACKAGE_LIST = {
        'openjdk-11-headless': ['openjdk-11-jre-headless']
    }

    # Extra packages that follow Java, e.g. openssl for cert generation
    EXTRA_PACKAGES = [
       'openssl'
    ]
    ks = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self.ks.set_default(ks_password=genRandomPassword())
        self.ks.set_default(ts_password=genRandomPassword())

    def _generate_keystores(self, elems):
        """Generate the keystores for each of the ssl keys available.

        Receives a list of lists named elems. Each element of elems is a list
        composed of the following contents:
        - CERT: The StoredState content for the ssl_cert
        - KEY:  The StoredState content for the ssl_key
        - PWD:  The password to be used to generate the PKCS#12 key and the
                actual keystore
        - GET_CERT: The get_ssl_cert method used to find the correct path
                    of for the certificate. It can be a config upload or
                    generated through certificates relation
        - GET_KEY:  Likewise, get_ssl_key method in charge of returning the
                    key, if that is set as a config or via relations
        - GET_KEYSTORE: path to be used to save the keystore.

        Config 'regenerate-keystore-truststore' must exist on the final charm.

        If CERT / KEY has the same values as GET_CERT / GET_KEY, it means
        the keystore has been already created and it is not necessary to do
        anything.

        Args:
        - elems: list of lists as described above
        """

        # INDICES WITHIN THE LIST:
        CERT, KEY, PWD, GET_CERT, GET_KEY, GET_KEYSTORE = 0, 1, 2, 3, 4, 5
        # Generate the keystores if cert/key exists
        for t in elems:
            if t[CERT] == t[GET_CERT]() and \
               t[KEY] == t[GET_KEY]():
                # Certs and keys are the same
                logger.info("Same certs and keys for {}".format(t[CERT]))
                continue
            t[CERT] = t[GET_CERT]()
            t[KEY] = t[GET_KEY]()
            if len(t[CERT]) > 0 and len(t[KEY]) > 0 and \
               len(t[PWD]) > 0 and len(t[GET_KEYSTORE]()) > 0:
                logger.info("Create PKCS12 cert/key for {}".format(t[CERT]))
                logger.debug("Iteration: {}".format(t))
                filename = genRandomPassword(6)
                PKCS12CreateKeystore(
                    t[GET_KEYSTORE](),
                    t[PWD],
                    t[GET_CERT](),
                    t[GET_KEY](),
                    user=self.config["user"],
                    group=self.config["group"],
                    mode=0o640,
                    openssl_chain_path="/tmp/" + filename + ".chain",
                    openssl_key_path="/tmp/" + filename + ".key",
                    openssl_p12_path="/tmp/" + filename + ".p12",
                    ks_regenerate=self.config.get(
                        "regenerate-keystore-truststore", False))
            elif not t[GET_KEYSTORE]():
                logger.debug("Keystore not found on Iteration: {}".format(t))

    def install_packages(self,
                         java_version='openjdk-11-headless',
                         packages=[]):
        """Installs openjdk and extra packages requested."""

        apt_update()
        apt_install(self.PACKAGE_LIST[java_version] +
                    self.EXTRA_PACKAGES +
                    packages)
