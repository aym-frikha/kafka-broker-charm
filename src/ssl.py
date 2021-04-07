## GOOD CANDIDATE TO MOVE TO SOMEWHERE ELSE
## TODO: Also add equivalent options for BCFKS trust/keystore
import os
import shutil

def _check_file_exists(path):
    try:
        os.stat(path)
    except FileNotFoundError:
        return False
    return True


def _generate_random_password(path):
    # TODO:
    pass

def RegisterIfKeystoreExists(path):
    return _check_file_exists(path)


def RegisterIfTruststoreExists(path):
    return _check_file_exists(path)


def SetTrustAndKeystoreFilePermissions(user, group, keystore_path,
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


def PKCS12CreateKeystore(kestore_path, keystore_pwd, ssl_cert, ssl_key):
    try:
        with open("/tmp/kafka-broker-charm-cert.chain", "w") as f:
            f.write(ssl_cert)
            f.close()
        with open("/tmp/kafka-broker-charm.key", "w") as f:
            f.write(ssl_key)
            f.close()
        pk12_cmd = ['openssl', 'pkcs12', '-export', '-in', "/tmp/kafka-broker-charm-cert.chain", "-inkey", "/tmp/kafka-broker-charm.key", "-out", "/tmp/kafka-broker-charm.p12", "-name", "localhost", "-passout", "pass:mykeypassword" ]
        subprocess.check_call(pk12_cmd)
        ks_cmd = ["keytool", "-importkeystore", "-srckeystore", "/tmp/kafka-broker-charm.p12", "-srcstoretype", "pkcs12", "-srcstorepass", "mykeypassword", "-destkeystore", keystore_path, "-deststoretype", "pkcs12", "-deststorepass", keystore_pwd, "-destkeypass", keystore_pwd]
        subprocess.check_call(ks_cmd)
    except Exception as e:
        # We've saved the key and cert to /tmp, we cannot leave it there
        # clean it up:
        os.remove("/tmp/kafka-broker-charm.key")
        os.remove("/tmp/kafka-broker-charm.p12")
        os.remove("/tmp/kafka-broker-charm-cert.chain")

def CreateTruststoreWithCertificates(ssl_ca, truststore_path):
    pass
    # TODO:
    ## 1) Convert ssl_ca into a cert chain
    ## 2) Split it into several certs
    ## 3) For each cert on the chain, save to a file push to keytool:
    ##    CHECK: https://github.com/confluentinc/cp-ansible/blob/b711fc9e3b43d2069a9ac8b13177e7f2a07c7bfb/roles/confluent.ssl/tasks/import_ca_chain.yml#L14
    ##    keytool -noprompt -keystore {{truststore_path}} -storetype pkcs12 -alias {fileName} -trustcacerts -import -file "$file" -deststorepass {{truststore_storepass}}


def CreateKeystoreAndTrustore(keystore_path,
                              truststore_path,
                              regenerate_stores):
    if RegisterIfKeystoreExists(keystore_path) and
       RegisterIfTruststoreExists(truststore_path) and
       not regenerate_stores:
       # return None as this option is not needed
       return None
    
    
