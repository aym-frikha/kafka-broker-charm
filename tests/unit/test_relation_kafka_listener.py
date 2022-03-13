"""Unit Test for Kafka Listener Relation class."""

import unittest
import json

from mock import (
    patch,
    PropertyMock
)

from charms.kafka_broker.v0.kafka_listener import (
    KafkaListenerProvidesRelation,
)


# Helper classes to allow mocking of some
# operator-specific objects
class Model:
    """Mock the model."""

    def __init__(self, app):
        """Init mock with app name."""
        self._app = app

    @property
    def app(self):
        """Return app name."""
        return self._app


class Relation:
    """Mock relation."""

    def __init__(self, data, app):
        """Create the mock relation."""
        self._app = app
        self._units = [Unit(False, self._app),
                       Unit(True, self._app),
                       Unit(False, self._app)]
        j = json.dumps(data)
        self._data = {
            self._app: {"request": j},
            self._units[0]: {"request": j},
            self._units[1]: {"request": j},
            self._units[2]: {"request": j}
        }

    @property
    def app(self):
        """Return the app name of the relation."""
        return self._app

    @property
    def data(self):
        """Return the data in the relation."""
        return self._data

    @property
    def units(self):
        """Return the unit list in the relation."""
        return self._units


class Event:
    """Mock event."""

    def __init__(self, relation):
        """Create the mock event."""
        self._relation = relation

    @property
    def relation(self):
        """Return the relation linked to the event."""
        return self._relation


class App:
    """Mock app."""

    def __init__(self, app):
        """Create the mock app."""
        self._app = app

    @property
    def name(self):
        """Return the app name."""
        return self._app


class Unit:
    """Mock unit."""

    def __init__(self, is_leader=False, app_name=""):
        """Create the mock unit."""
        self._is_leader = is_leader
        self._name = app_name

    def is_leader(self):
        """Return if mock is set as leader."""
        return self._is_leader

    @property
    def app(self):
        """Return the mock app for the unit."""
        return App(self._name)


class KafkaListenerRelationTest(unittest.TestCase):
    """Class that implements the unit tests for the kafka listener."""

    maxDiff = None

    def setUp(self):
        """Set up the kafka listener unittest."""
        super(KafkaListenerRelationTest, self).setUp()

    @patch.object(KafkaListenerProvidesRelation, "ts_pwd",
                  new_callable=PropertyMock)
    @patch.object(KafkaListenerProvidesRelation, "ts_path",
                  new_callable=PropertyMock)
    @patch.object(KafkaListenerProvidesRelation, "available_port",
                  new_callable=PropertyMock)
    @patch.object(KafkaListenerProvidesRelation, "_get_default_listeners")
    @patch.object(KafkaListenerProvidesRelation,
                  "relations", new_callable=PropertyMock)
    @patch.object(KafkaListenerProvidesRelation,
                  "unit", new_callable=PropertyMock)
    @patch.object(KafkaListenerProvidesRelation, "__init__")
    def test_get_listeners(self,
                           mock_init,
                           mock_unit,
                           mock_relations,
                           mock_get_default_lst,
                           mock_port,
                           mock_ts_path,
                           mock_ts_pwd):
        """Test getting the listeners via relation."""
        # __init__ must return None
        mock_init.return_value = None
        mock_unit.return_value = Unit(True)
        mock_port.return_value = 9092
        mock_ts_path.return_value = "test"
        mock_ts_pwd.return_value = "test"
        mock_relations.return_value = [Relation({
            "is_public": False,
            "plaintext_pwd": "",
            "secprot": "PLAINTEXT",
            "SASL": {},
            "cert": ""
        }, app=App("test")), Relation({
            "is_public": True,
            "plaintext_pwd": "",
            "secprot": "SASL_SSL",
            "SASL": {
                "protocol": "GSSAPI",
                "kerberos-principal": "principal",
                "kerberos-protocol": "http"
            },
            "cert": ""
        }, app=App("connect"))]
        obj = KafkaListenerProvidesRelation(None, "listener")
        # Given the __init__ has been replaced with a mock object, manually
        # set the port value:
        obj._port = mock_port.return_value
        obj.get_unit_listener("", "", False, False)
        self.assertEqual(obj.get_unit_listener("", "", False, False), '{"test": {"bootstrap_server": "*BINDING*:9092", "port": 9092, "endpoint": "test://*BINDING*:9092", "advertise": "test://*BINDING*:9092", "secprot": "PLAINTEXT", "SASL": {}, "plaintext_pwd": "", "cert_present": false, "sasl_present": true, "ts_path": "test", "ts_pwd": "test", "ks_path": "", "ks_pwd": "", "clientauth": false}, "connect": {"bootstrap_server": "*ADVERTISE*:9092", "port": 9092, "endpoint": "connect://*ADVERTISE*:9092", "advertise": "connect://*ADVERTISE*:9092", "secprot": "SASL_SSL", "SASL": {"protocol": "GSSAPI", "kerberos-principal": "principal", "kerberos-protocol": "http"}, "plaintext_pwd": "", "cert_present": false, "sasl_present": true, "ts_path": "test", "ts_pwd": "test", "ks_path": "", "ks_pwd": "", "clientauth": false}}') # noqa
