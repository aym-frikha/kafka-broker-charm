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
import shutil
import string
import subprocess
from OpenSSL import crypto

CHARS_PASSWORD = string.ascii_letters + string.digits
PASSWORD_LEN = 48


nano = [
    '_break_crt_chain',
    'saveCrtChainToFile',
    '_check_file_exists',
    'genRandomPassword',
    'RegisterIfKeystoreExists',
    'RegisterIfTruststoreExists',
    'setFilePermissions',
    'generateSelfSigned',
    'SetTrustAndKeystoreFilePermissions',
    'SetCertAndKeyFilePermissions',
    'PKCS12CreateKeystore',
    'CreateTruststoreWithCertificates',
    'CreateKeystoreAndTruststore',
    'CreateTruststore'
]


def _break_crt_chain(buffer):
    return [i+"-----END CERTIFICATE-----\n"
            for i in buffer.split("-----END CERTIFICATE-----\n")
            if i.startswith("-----BEGIN CERTIFICATE-----\n")]


def saveCrtChainToFile(buffer,
                       cert_path,
                       ca_chain_path,
                       user=None,
                       group=None,
                       force=False):
    crts = _break_crt_chain(buffer)
    if _check_file_exists(cert_path) and not force:
        raise Exception("{} already exists, aborting".format(cert_path))
    if _check_file_exists(ca_chain_path) and not force:
        raise Exception("{} already exists, aborting".format(ca_chain_path))
    # cert_path can be set to None, and all the files will the
    # certificates will be saved to ca_chain_path
    if cert_path:
        with open(cert_path, "w") as f:
            f.write(crts[0])
            f.close()
        with open(ca_chain_path, "w") as f:
            f.write("\n".join(crts[1:]))
            f.close()
    else:
        with open(cert_path, "w") as f:
            f.write("\n".join(crts[0:]))
            f.close()
    if user and group:
        setFilePermissions(cert_path, user, group, mode=0o640)
        setFilePermissions(ca_chain_path, user, group, mode=0o640)


def _check_file_exists(path):
    try:
        os.stat(path)
    except FileNotFoundError:
        return False
    return True


# NOTE(pguimaraes):
# DO NOT change length default value below without updating
# PASSWORD_LEN constant at the top of ssl.py
def genRandomPassword(length=48):
    return "".join(CHARS_PASSWORD[c % len(CHARS_PASSWORD)]
                   for c in os.urandom(length))


def RegisterIfKeystoreExists(path):
    return _check_file_exists(path)


def RegisterIfTruststoreExists(path):
    return _check_file_exists(path)


def setFilePermissions(path, user, group, mode=None):
    shutil.chown(path, user=user, group=group)
    if mode:
        os.chmod(path, mode)


def generateSelfSigned(folderpath=None,
                       certname=None,
                       keysize=4096,
                       cn="*.maas",
                       user=None,
                       group=None,
                       mode=None):
    key = crypto.PKey()
    key.generate_key(crypto.TYPE_RSA, keysize)
    cert = crypto.X509()
    serNum = 0
    valSecs = 10*365*24*60*60
    x509name = crypto.X509Name(crypto.X509().get_subject())
    x509name.countryName = "UK"
    x509name.stateOrProvinceName = "London"
    x509name.organizationName = "TestWandUbuntu"
    x509name.organizationalUnitName = "WandLib"
    x509name.commonName = cn or "*.example.com"
    cert.set_subject(x509name)
    cert.set_serial_number(serNum)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(valSecs)
    cert.set_pubkey(key)
    issuerName = crypto.X509Name(x509name)
    cert.set_issuer(issuerName)
    cert.sign(key, 'sha512')
    cname = certname or genRandomPassword(6)
    folder = folderpath or "/tmp"
    with open(os.path.join(folder, cname + ".crt"), "w") as f:
        f.write(crypto.dump_certificate(
            crypto.FILETYPE_PEM, cert).decode("utf-8"))
        f.close()
    with open(os.path.join(folder, cname + ".key"), "w") as f:
        f.write(crypto.dump_privatekey(
            crypto.FILETYPE_PEM, key).decode("utf-8"))
        f.close()
    if user and group:
        setFilePermissions(os.path.join(folder, cname + ".crt"),
                           user, group, mode)
        setFilePermissions(os.path.join(folder, cname + ".key"),
                           user, group, mode)
    return (crypto.dump_certificate(
               crypto.FILETYPE_PEM, cert).decode("utf-8"),
            crypto.dump_privatekey(
                crypto.FILETYPE_PEM, key).decode("utf-8"))


def SetTrustAndKeystoreFilePermissions(user, group,
                                       keystore_path,
                                       truststore_path):
    shutil.chown(keystore_path, user=user, group=group)
    os.chmod(keystore_path, 0o640)
    shutil.chown(truststore_path, user=user, group=group)
    os.chmod(truststore_path, 0o640)


def SetCertAndKeyFilePermissions(user, group,
                                 ca_cert_path,
                                 cert_path,
                                 key_path):
    shutil.chown(ca_cert_path, user=user, group=group)
    os.chmod(ca_cert_path, 0o640)
    shutil.chown(cert_path, user=user, group=group)
    os.chmod(cert_path, 0o640)
    shutil.chown(key_path, user=user, group=group)
    os.chmod(key_path, 0o640)


def PKCS12CreateKeystore(keystore_path,
                         keystore_pwd,
                         ssl_chain,
                         ssl_key,
                         user=None,
                         group=None,
                         mode=None,
                         openssl_chain_path="/tmp/ks-charm-cert.chain",
                         openssl_key_path="/tmp/ks-charm.key",
                         openssl_p12_path="/tmp/ks-charm.p12",
                         ks_regenerate=False):

    # We've saved the key and cert to /tmp, we cannot leave it there
    # clean it up:
    def __cleanup():
        for i in [openssl_chain_path, openssl_key_path, openssl_p12_path]:
            try:
                os.remove(i)
            except Exception:
                pass
    if ks_regenerate:
        try:
            os.remove(keystore_path)
        except Exception:
            pass
    try:
        with open(openssl_chain_path, "w") as f:
            f.write(ssl_chain)
            f.close()
        with open(openssl_key_path, "w") as f:
            f.write(ssl_key)
            f.close()
        pk12_cmd = ['openssl', 'pkcs12', '-export', '-in',
                    openssl_chain_path,
                    "-inkey", openssl_key_path,
                    "-out", openssl_p12_path,
                    "-name", "localhost", "-passout",
                    "pass:{}".format(keystore_pwd)]
        subprocess.check_call(pk12_cmd)
        ks_cmd = ["keytool", "-importkeystore", "-srckeystore",
                  openssl_p12_path, "-srcstoretype",
                  "pkcs12", "-srcstorepass", keystore_pwd,
                  "-destkeystore", keystore_path, "-deststoretype", "pkcs12",
                  "-deststorepass", keystore_pwd, "-destkeypass", keystore_pwd]
        subprocess.check_call(ks_cmd)
        setFilePermissions(keystore_path, user, group, mode)
    except Exception as e:
        __cleanup()
        raise e
    # We clean up intermediate keys and certs for p12 anyway
    __cleanup()


def CreateTruststore(ts_path,
                     ts_pwd,
                     ts_certs,
                     ts_regenerate=False,
                     user=None,
                     group=None,
                     mode=None):
    crtpath = "/tmp/juju_ca_cert"
    if ts_regenerate:
        try:
            os.remove(ts_path)
        except Exception:
            pass
    # small random string to act as prefix for the certs' alias
    host_rand = genRandomPassword(6)
    counter = 0
    for c in ts_certs:
        with open(crtpath, "w") as f:
            f.write(c)
            f.close()
        # ZK doc: alias must change per cert added
        ts_cmd = ["keytool", "-noprompt", "-keystore", ts_path,
                  "-storetype", "pkcs12", "-alias",
                  "host.{}.{}".format(host_rand, counter),
                  "-trustcacerts", "-import", "-file", crtpath,
                  "-deststorepass", ts_pwd]
        subprocess.check_call(ts_cmd)
        setFilePermissions(ts_path, user, group, mode)
        counter += 1
    os.remove(crtpath)
