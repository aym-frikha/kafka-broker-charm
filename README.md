# kafka-broker

## Description

Kafka Broker Charm is the baremetal charm for Confluent Kafka.

Get a Kafka cluster automated and manage its lifecycle with Juju and charms.

## Usage

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

WIP: Apache distro is still under implementation.

### Data Folders

Some types of services of kafka demands directories to store data, such as kafka brokers, ksqldb and, in Confluent case, Confluent Center.

There are some options on how to configure the folders correctly for your service. First option is to use charm configs. That allows to either specify existing folders or folder + disks, in which the disk
will zapped with a new filesystem and mounted to the folder. Dedicating a disk is optional, one can also just specify a folder in the rootfs of your system.

Here are some configuration examples:

1) Just specify a folder to be created under rootfs and used as data in Kafka charm. In the example below, a filesystem will be specified as well, but that value will be ignored given a disk is not 
passed as well.

```

kafka:
  options:
    data-log-dir: |
      ext4: /data

```

2) Specify a folder and a device (vdc for /log and vdd for /data):

```

kafka:
  options:
    data-log-dir: |
      ext4: /log
      xfs: /data
    data-log-device:
      - /dev/vdc
      - /dev/vdd

```

WIP: The last option is to use the storage backend provided by Juju. In that case, up to 32 disks can be specified, and they will be mounted as XFS and each directory will be named /data{1..32}.

WARNING: For production scenarios, it is recommended to use dedicated disks for data.

### Certificate management

Kafka uses a keystore to contain the certificate and keys for TLS. Besides, it uses a truststore with all the trusted certificates for each unit.

Truststore should always be set since relations may use certificates that are not trusted by default by Java's upstream truststores.
Units will learn certificate information from its peers and across relations and store them in the truststore.

If keystore is set to empty, certificates will not be used for that listener.

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

    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements-dev.txt
