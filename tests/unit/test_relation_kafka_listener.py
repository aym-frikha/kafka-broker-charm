import unittest
import json

from mock import (
    patch,
    PropertyMock
)

from wand.apps.relations.kafka_listener import (
    KafkaListenerProvidesRelation,
)


# Helper classes to allow mocking of some
# operator-specific objects
class model:
    def __init__(self, app):
        self._app = app

    @property
    def app(self):
        return self._app


class relation:
    def __init__(self, data, app):
        self._app = app
        self._units = [unit(False, self._app),
                       unit(True, self._app),
                       unit(False, self._app)]
        j = json.dumps(data)
        self._data = {
            self._app: {"request": j},
            self._units[0]: {"request": j},
            self._units[1]: {"request": j},
            self._units[2]: {"request": j}
        }

    @property
    def app(self):
        return self._app

    @property
    def data(self):
        return self._data

    @property
    def units(self):
        return self._units


class event:
    def __init__(self, relation):
        self._relation = relation

    @property
    def relation(self):
        return self._relation


class app:
    def __init__(self, app):
        self._app = app

    @property
    def name(self):
        return self._app


class unit:
    def __init__(self, is_leader=False, app_name=""):
        self._is_leader = is_leader
        self._name = app_name

    def is_leader(self):
        return self._is_leader

    @property
    def app(self):
        return app(self._name)


class KafkaListenerRelationTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
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
        # __init__ must return None
        mock_init.return_value = None
        mock_unit.return_value = unit(True)
        mock_port.return_value = 9092
        mock_ts_path.return_value = "test"
        mock_ts_pwd.return_value = "test"
        mock_relations.return_value = [relation({
            "is_public": False,
            "plaintext_pwd": "",
            "secprot": "PLAINTEXT",
            "SASL": {},
            "cert": ""
        }, app=app("test")), relation({
            "is_public": True,
            "plaintext_pwd": "",
            "secprot": "SASL_SSL",
            "SASL": {
                "protocol": "GSSAPI",
                "kerberos-principal": "principal",
                "kerberos-protocol": "http"
            },
            "cert": ""
        }, app=app("connect"))]
        obj = KafkaListenerProvidesRelation(None, "listener")
        # Given the __init__ has been replaced with a mock object, manually
        # set the port value:
        obj._port = mock_port.return_value
        obj.get_unit_listener("", "", False, False)
        self.assertEqual(obj.get_unit_listener("", "", False, False), '{"test": {"bootstrap_server": "*BINDING*:9092", "port": 9092, "endpoint": "test://*BINDING*:9092", "advertise": "test://*BINDING*:9092", "secprot": "PLAINTEXT", "SASL": {}, "plaintext_pwd": "", "cert_present": false, "sasl_present": true, "ts_path": "test", "ts_pwd": "test", "ks_path": "", "ks_pwd": "", "clientauth": false}, "connect": {"bootstrap_server": "*ADVERTISE*:9092", "port": 9092, "endpoint": "connect://*ADVERTISE*:9092", "advertise": "connect://*ADVERTISE*:9092", "secprot": "SASL_SSL", "SASL": {"protocol": "GSSAPI", "kerberos-principal": "principal", "kerberos-protocol": "http"}, "plaintext_pwd": "", "cert_present": false, "sasl_present": true, "ts_path": "test", "ts_pwd": "test", "ks_path": "", "ks_pwd": "", "clientauth": false}}') # noqa
