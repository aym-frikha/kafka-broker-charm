#!/usr/bin/env python3
# Copyright 2021 pguimaraes
# See LICENSE file for licensing details.

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

    https://discourse.charmhub.io/t/4208
"""

import subprocess
import logging

from ops.charm import CharmBase
from ops.main import main
from ops.framework import StoredState

logger = logging.getLogger(__name__)

OPENJDK11_PACKAGES = []

PREREQS_PACKAGES = OPENJDK11_PACKAGES + [
    'openssl',
]

# Given: https://docs.confluent.io/current/installation/cp-ansible/ansible-configure.html
# Setting confluent-server by default
CONFLUENT_PACKAGES = [
  "confluent-common",
  "confluent-rest-utils",
  "confluent-metadata-service",
  "confluent-ce-kafka-http-server",
  "confluent-kafka-rest",
  "confluent-server-rest",
  "confluent-telemetry",
  "confluent-server",
  "confluent-rebalancer",
  "confluent-security",
]

LATEST_VERSION_CONFLUENT = "6.1"

class KafkaBrokerCharm(CharmBase):
    """Charm the service."""

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.fortune_action, self._on_fortune_action)
        self._stored.set_default(things=[])

    def _on_install(self, _):
        distro = self.config.get("distro", "confluent").lower()
        version = self.config.get("version", LATEST_VERSION_CONFLUENT)
        if distro == "confluent":
            key = subprocess.check_output(['wget', '-qO','-',
                                           'https://packages.confluent.io/deb/{}/archive.key'.format(version)])
            apt_source(
                'deb [arch=amd64] https://packages.confluent.io/deb/{} stable main'.format(version),
                key=key)
            apt_install(CONFLUENT_PACKAGES)
        elif distro == "apache":
            raise Exception("Not Implemented Yet")

    def _on_config_changed(self, _):
        # Note: you need to uncomment the example in the config.yaml file for this to work (ensure
        # to not just leave the example, but adapt to your configuration needs)
        current = self.config["thing"]
        if current not in self._stored.things:
            logger.debug("found a new thing: %r", current)
            self._stored.things.append(current)

    def _on_fortune_action(self, event):
        # Note: you need to uncomment the example in the actions.yaml file for this to work (ensure
        # to not just leave the example, but adapt to your needs for actions commands)
        fail = event.params["fail"]
        if fail:
            event.fail(fail)
        else:
            event.set_results({"fortune": "A bug in the code is worth two in the documentation."})


if __name__ == "__main__":
    main(KafkaBrokerCharm)
