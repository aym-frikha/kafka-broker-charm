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

import os
import unittest
# from mock import patch
# from mock import PropertyMock

# from OpenSSL import crypto, SSL
# import jks

from mock import patch

from .test_cert import UBUNTU_COM_CERT

import charms.kafka_broker.v0.kafka_security as security


class TestSecurity(unittest.TestCase):

    def setUp(self):
        super(TestSecurity, self).setUp()

    def test_break_crt_chain(self):
        self.assertEqual(2, len(security._break_crt_chain(UBUNTU_COM_CERT)))

    def test_gen_self_signed(self):
        security.generateSelfSigned("/tmp", "testcert")
        self.assertEqual(True,
                         security._check_file_exists("/tmp/testcert.crt"))
        self.assertEqual(True,
                         security._check_file_exists("/tmp/testcert.key"))

    @patch.object(security, "setFilePermissions")
    def test_create_ks_ts(self,
                          mock_file_perms):
        def __cleanup():
            for i in ["/tmp/testcert.crt", "/tmp/testcert.key",
                      "/tmp/testks.jks", "/tmp/testts.jks",
                      "/tmp/ks-charm*"]:
                try:
                    os.remove(i)
                except Exception:
                    pass
        __cleanup()
        ks_pwd = security.genRandomPassword()
        ts_pwd = security.genRandomPassword()
        self.assertEqual(security.PASSWORD_LEN, len(ks_pwd))
        crt, key = security.generateSelfSigned("/tmp", "testcert")
        # Mocking setFilePermissions to avoid making this test too dependent
        # on the user running it
        security.PKCS12CreateKeystore("/tmp/testks.jks",
                                      ks_pwd,
                                      crt, key)
        security.CreateTruststore("/tmp/testts.jks",
                                  ts_pwd, [crt], True)
        self.assertEqual(True, security._check_file_exists("/tmp/testks.jks"))
        self.assertEqual(True, security._check_file_exists("/tmp/testts.jks"))
        __cleanup()
