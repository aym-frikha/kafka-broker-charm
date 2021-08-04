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

### Certificate management

Kafka uses a keystore to contain the certificate and keys for TLS. Besides, it uses a truststore with all the trusted certificates for each unit. Each listener gets its own keystore / truststore.

TODO: If keystore is set to empty, certificates will not be used for that listener.

### JMX Exporter support

Kafka's JVM can export information to Prometheus. Setup the integration
with the following options:

```
    jmx_exporter_version: v0.12
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
