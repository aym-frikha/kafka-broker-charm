from ops.charm import CharmBase

from charmhelpers.fetch import (
    apt_update,
    add_source,
    apt_install
)


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
