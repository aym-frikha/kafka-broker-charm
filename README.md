# kafka-broker

## Description

Kafka Broker Charm is the baremetal charm for Confluent Kafka.

Get a Kafka cluster automated and manage its lifecycle with Juju and charms.

## Setup

See ```config.yaml``` for the full list of options and descriptions.

### Adding extra configurations

Kafka broker charms allows to append configurations to config files such as ```server.properties``` or service ```override.conf```.

It is yaml-formatted list of ```key: value``` field. Can be added in a bundle with:

```
    kafka-broker:
        charm: cs:kafka
        options:
            server-properties: |
                group.initial.rebalance.delay.ms: 3000
                log.retention.check.interval.ms: 300000
```

Or using CLI, as for example:

```
    $ juju config kafka-broker server-properties="group.initial.rebalance.delay.ms: 3000
      log.retention.check.interval.ms: 300000"
```

IMPORTANT: in case of a conflict between option OR charm's decision as for a relation, for example; the relation setup (i.e. the value defined by the charm for that given configuration) will be used instead of the value set in the option.

### Relations translated to Kafka

Similar to how charms work, Kafka also allows to choose different interfaces and certificates for a given connection.

In Kafka, that is known as listener.

For example, brokers and REST units will communicate on an internal network, then the listener between these two applications is set to the internal IPs.

Therefore, listeners are managed according to spaces.

### Distros

Kafka charms accept several types of distros: confluent, apache and apache_snap. Confluent will select to use confluent packages and demands a license key from confluent to correctly setup the environment.
Apache_snap uses the upstream code available for Kafka clusters. That code has been compiled into snaps and can be readily deployed.

WIP: "apache" distro is not yet available.

### Data Folders

Some types of services of kafka demands directories to store data, such as kafka brokers, ksqldb and, in Confluent case, Confluent Center.

List of dictionaries where each entry represents a new folder to be used in log.dirs. The overall format expected is:
```
[
    {
        "fs_path": <PATH>,
        "device": {
            "name": <DEVICE_PATH>,
            "filesystem": <OPTIONAL, FS_MODEL>,
            "options": <OPTIONAL, FS_MOUNT_OPTIONS>
        }, ## OPTIONAL
    }, ...
]
```

If only "fs_path" is specified, then the charm uses the rootfs and just creates the new folder. 
Device section is optional and allows to pass a disk to be used when mounting the folder path.
Within device section, "name" is mandatory and other fields are optional.
Following config is valid, for example:
```
[
    {
        "fs_path": /data/1,
    },
    {
        "fs_path": /data/2,
        "device": {
            "name": /dev/sdb,
        },
    },
    {
        "fs_path": /data/3,
        "device": {
            "name": /dev/sdc,
            "filesystem": ext4,
        }
    }
]
```

That will create 3x log.dirs for Kafka, where /data/1 will be placed on the same disk as the rootfs, /data/2 will be flushed with default filesystem (xfs) on /dev/sdb and /data/3 specifies more details of what the device should look like.
Changing this configuration will trigger the charm to mount & format a device / umount any devices that were changed in the list.

WARNING: For production scenarios, it is recommended to use dedicated disks for data.

### Certificate management

Kafka uses a keystore to contain the certificate and keys for TLS. Besides, it uses a truststore with all the trusted certificates for each unit.

Truststore should always be set since relations may use certificates that are not trusted by default by Java's upstream truststores.
Units will learn certificate information from its peers and across relations and store them in the truststore.

If keystore is set to empty, certificates will not be used for that relation.

#### Manage Certificates within the Truststore

If Truststore is specified, users can add certificates to be globally trusted by an application. The add-/remove-/list-certificates applies to all the truststores available for this charm.

Users can add certificates to truststore using:

```
    $ juju run-action add-certificate certs-files=<comma-separated-list-of-certs-to-add>
```

If the user wants to empty out the truststore of any custom added certs, run:

```
    $ juju run-action remove-certificates certs-files=<comma-separated-list-of-certs-to-remove
```

If the user wants to list manually added certs to the Truststore:

```
    $ juju run-action list-certificates
```

### JMX Exporter support

Kafka's JVM can export information to Prometheus. Setup the integration
with the following options:

```
    jmx_exporter_version: 0.12
    jmx_exporter_url: Maven's URL from where JMX jar can be downloaded.
        Format it to replace the versions.
    jmx-exporter-port: Port to be exposed by the exporter for prometheus.
    jmx_exporter_labels: comma-separated list of key=value tags.
    jmx_exporter_use_internal: use internal endpoint of Prometheus relation
```

The setup above will render the option in override.conf service:

```
    -javaagent:/opt/prometheus/jmx_prometheus_javaagent.jar=9409:/opt/prometheus/prometheus.yml
```

### Nagios Integration

Set ```nagios_context``` config to allow NRPE integration to work. This option is used as a prefix for the check names and should change per environment.

### Authentication with Kerberos

To set Kerberos, there are two steps that needs to be taken into consideration. First step, set the correct configuration on:

```
    kerberos-protocol
    kerberos-realm
    kerberos-domain
    kerberos-kdc-hostname
    kerberos-admin-hostname
```

Once the units are deployed, they will be blocked, waiting for the keytab file. That should be added per-unit, according to its hostname, using actions. Check the actions documentation for more details.

### Sysctl tuning

As proposed in [Ansible for Kafka Broker](https://github.com/confluentinc/cp-ansible/blob/8daf3140882ddbe84cecf0320c52592374a1a66e/roles/confluent.kafka_broker/defaults/main.yml#L51), there are some sysctl settings that are necessary for production-grade cluster.

In the charmed version of Kafka stack, this can be achieved with sysconfig charm:

```
  sysconfig:
    charm: cs:sysconfig
    options:
      sysctl: "{
          vm.swappiness: 1,
          vm.dirty_background_ratio: 5,
          vm.dirty_ratio: 80,
          vm.max_map_count: 262144
        }"

...

relations:
- - sysconfig
  - kafka-broker:juju-info
```

## Developing

Create and activate a virtualenv with the development requirements:

```
    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements-dev.txt
```

### Certificate and Key Management

The charm will generate keystores and truststores separately for spaces whenever it is possible. If the application supports, for example, different stores for a schema-registry and listener relations,
then generate one per relation. In Kafka, keystores will be relevant whenever the unit must authenticate via TLS. That can happen if the unit is acting as a server (e.g. Kafka Broker) or 
if mutual authentication for TLS has been implemented.


Java does come with default Truststores which contain widely trusted CAs. However, implementing its own truststores allows the operator to control if they want to use self-signed certs across the stack.

Therefore, if the operator specifies a keystore and truststore, then they should be configured instead of using openjdk's defaults.

The logic to decide if a keystore or truststore will be used is similar to (remember, keystore definition is the gate to decide if TLS will be used or not):

```
if len(self.get_ssl_keystore()) > 0:
    # User specified a keystore, check if cert/key are also configured
    if len(self.get_ssl_key()) > 0 and len(self.get_ssl_cert()) > 0:
        # Start logic to generate the keystore

    # Now, a custom cert has been specified, create a store
    if len(self.get_ssl_truststore()) > 0 and len(self.get_ssl_cert()) > 0:
        CreateTruststore(...)
```

Besides the certificate specified via config option OR tls-certificates, the charm needs to share that certificate with its relation peers and add any cert specified by the user.

```

    if len(self.get_ssl_truststore()) > 0 and len(self.get_ssl_cert()) > 0:
        crt_list = [self.get_ssl_cert()]

        # Find all the certs shared across the relation
        for r in self.RELATION.relations:
            r.data[self.unit]["tls-cert"] = self.get_ssl_cert()

            for u in r.units:
                if "tls-cert" in r.data[u]:
                    crt_list.append(r.data[u]["tls-cert"])

        # Now, recover user-defined certs passed via action
        if self.ks.ssl_certs:
            crt_list.extend(self.ks.ssl_certs)

        CreateTruststore(...)
```

Keystores will be generated using JavaCharmBase ```_generate_keystores``` method. 

For that, the charm code must specify a list of lists, each sub-list contain methods and data to generate the Keystore, example:

```
    def _generate_keystores(self):
        """Generate the keystores using JavaCharmBase method"""

        ks = [[self.ks.ssl_cert, self.ks.ssl_key, self.ks.ks_password,
               self.get_ssl_cert, self.get_ssl_key, self.get_ssl_keystore],

              [self.ks.ssl_sr_cert, self.ks.ssl_sr_key, self.ks.ks_sr_pwd,
               self.get_ssl_sr_cert, self.get_ssl_sr_key,
               self.get_ssl_sr_keystore],

              [self.ks.ssl_listener_cert, self.ks.ssl_listener_key,
               self.ks.ks_listener_pwd,
               self.get_ssl_listener_cert, self.get_ssl_listener_key,
               self.get_ssl_listener_keystore]]
        # Call the method from JavaCharmBase
        super()._generate_keystores(ks)
```
## Operations

### Charm Upgrade

Run the ```juju upgrade-charm````command to trigger the unit's upgrade.

The charm upgrade runs a config-changed.

### Application Upgrade

The upgrade of an application unit is always triggered manually, using ```upgrade``` action.

The user must first either choose a new version of the application to be upgrade as follows:

```
  # Upgrade from 3.0 -> 3.1 with Snaps
  $ juju config kafka version="3.1/stable" # selected a new channel

  # Upgrade 6.1 -> 7.0 with Confluent
  $ juju config kafka version="7.0" # selected new package repo
```

Once the channel has been chosen, run the upgrade action:

```
$ juju run-action --wait kafka/<unit> upgrade
```

For snaps, upgrade is only possible on:
* resources: only if you add more resources
* version option: when moving the option to the next step.

There is no checks for versions, so a downgrade is also possible.

The upgrade action will trigger a restart of the application.

ATTENTION: Confluent package upgrades is still experimental.
