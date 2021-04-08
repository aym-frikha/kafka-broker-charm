import logging

from ops.charm import CharmBase
from ops.model import BlockedStatus, MaintenanceStatus, ActiveStatus

from charmhelpers.fetch import (
    apt_update,
    add_source,
    apt_install
)

from charmhelpers.core import (
    render,
    mount
)

logger = logging.getLogger(__name__)


class JavaCharmBase(CharmBase):

    PACKAGE_LIST = {
        'openjdk-11-headless': [ 'openjdk-11-jre-headless' ]
    }

    # Extra packages that follow Java, e.g. openssl for cert generation
    EXTRA_PACKAGES = [
       'openssl'
    ]

    def __init__(self, *args):
        super().__init__(*args)

    def install_packages(self, java_version='openjdk-11-headless'):
        apt_update()
        apt_install(self.PACKAGE_LIST[java_verion] + self.EXTRA_PACKAGES)



class KafkaJavaCharmBase(JavaCharmBase):

    LATEST_VERSION_CONFLUENT = "6.1"

    @property
    def distro(self):
        return self.options.get("distro","confluent").lower()

    def __init__(self, *args):
        super().__init__(*args)

    def install_packages(self, java_version, packages):
        version = self.config.get("version", self.LATEST_VERSION_CONFLUENT)
        if self.distro == "confluent":
            key = subprocess.check_output(['wget', '-qO','-',
                                           'https://packages.confluent.io/deb/{}/archive.key'.format(version)])
            add_source(
                'deb [arch=amd64] https://packages.confluent.io/deb/{} stable main'.format(version),
                key=key)
            apt_update()
        elif self.distro == "apache":
            raise Exception("Not Implemented Yet")

        apt_install(packages)


    def is_ssl_enabled(self):
        if len(self.config.get("ssl_ca","")) > 0 and
           len(self.config.get("ssl_cert","")) > 0 and
           len(self.config.get("ssl_key","")) > 0:
            return True
        if len(self.config.get("ssl_ca","")) > 0 or
           len(self.config.get("ssl_cert","")) > 0 or
           len(self.config.get("ssl_key","")) > 0:
            logger.warning("Only some of the ssl configurations have been set")
        return False

    def is_sasl_kerberos_enabled(self):
        # TODO: implement this logic
        return False

    def is_sasl_digest_enabled(self):
        # TODO: implement this logic
        return False

    def is_jolokia_enabled(self):
        # TODO: implement this logic
        return False

    def is_jmxexporter_enabled(self):
        # TODO: implement this logic
        return False

    def create_data_and_log_dirs(self, data_log_dev,
                                 data_dev,
                                 data_log_dir,
                                 data_dir,
                                 data_log_fs,
                                 data_fs,
                                 user=root,
                                 group=root,
                                 fs_options=None):

        if len(data_log_dir or "") == 0:
            logger.warning("Data log dir config empty")
            BlockedStatus("data-log-dir missing, please define it")
            return
        if len(data_dir or "") == 0:
            logger.warning("Data dir config empty")
            BlockedStatus("data-dir missing, please define it")
            return
        os.mkdir(data_log_dir, 0o750)
        shutil.chown(data_log_dir,
                     user=self.config["zookeeper-user"],
                     group=self.config["zookeeper-group"])
        os.mkdir(data_dir, 0o750)
        shutil.chown(data_dir,
                     user=self.config["zookeeper-user"],
                     group=self.config["zookeeper-group"])
        dev, fs = None, None
        if len(data_log_dev or "") == 0:
            log.warning("Data log device not found, using rootfs instead")
        else:
            for k,v in yaml.safe_load(data_log_dev):
                fs = k
                dev = v
            log.info("Data log device: mkfs -t {}".format(fs))
            cmd = ["mkfs", "-t", fs, dev]
            subprocess.check_call(cmd)
            mount(dev, data_log_dir,
                  options=self.config.get("fs-options", None),
                  persist=True, filesystem=fs)

        if len(data_dev or "") == 0:
            logger.warning("Data device not found, using rootfs instead")
        else:
            for k,v in yaml.safe_load(data_dev):
                fs = k
                dev = v
            logger.info("Data log device: mkfs -t {}".format(fs))
            cmd = ["mkfs", "-t", fs, dev]
            subprocess.check_call(cmd)
            mount(dev, data_dir,
                  options=self.config.get("fs-options", None),
                  persist=True, filesystem=fs)
