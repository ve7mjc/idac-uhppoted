from literals import DEFAULT_UHPPOTED_MQTT_TOPIC_ROOT

from dataclasses import dataclass, field, is_dataclass
from typing import Optional, Any, Union, Type, TypeVar
import os
import logging
# from pprint import pprint

from dotenv import load_dotenv
import yaml


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig()


@dataclass
class MqttConfiguration:
    host: str
    username: str
    password: str
    port: Optional[int] = 1883


@dataclass
class UhppoteDeviceConfiguration:
    device_id: str
    label: Optional[str] = "UHPP Controller"


@dataclass
class UhppoteServerConfiguration:
    mqtt_topic_root: Optional[str] = DEFAULT_UHPPOTED_MQTT_TOPIC_ROOT
    devices: list[UhppoteDeviceConfiguration] = field(default_factory=list)
    client_id: Optional[str] = "idac"


@dataclass
class MembershipPortalConfiguration:
    url_get_tokens_list: str
    url_put_token_events: str


@dataclass
class Configuration:
    membership_portal: MembershipPortalConfiguration
    mqtt: MqttConfiguration
    uhppoted: UhppoteServerConfiguration
    devices: list[UhppoteDeviceConfiguration] = field(default_factory=list)


class ConfigBuilder:

    dicts: list[dict]

    def __init__(self, dicts: list[dict] = []) -> None:
        self.dicts = dicts

        # load variables from {cwd}/.env into environment
        load_dotenv()

    def add_yaml(self, yaml_path: str) -> None:

        if not os.path.exists(yaml_path):
            raise FileNotFoundError(yaml_path)

        with open(yaml_path, 'r') as stream:
            logger.debug("loading and parsing yaml from '%s'", yaml_path)
            try:
                self.dicts.append(yaml.safe_load(stream))
            except yaml.YAMLError as exc:
                print(exc)

    def get_key(self, key: str, default: Optional[Any] = None) -> Any:
        """
            get_key('some.config.key') retrieves:
                - dict['some']['config']['key']
                - ENV variable 'SOME_CONFIG_KEY'
        """
        keys = key.split(".")

        # search paths -- TODO: search order
        source_dict: dict[str, Any]
        for source_dict in self.dicts:
            lev = source_dict
            for k in keys:
                try:
                    lev = lev[k]
                except KeyError:
                    lev = None
                    continue
            if lev:
                return lev

        # Search Environment Variables
        env_key = key.replace('.', '_').upper()
        return os.environ.get(env_key, default)


def get_config() -> Configuration:

    builder = ConfigBuilder()

    # search for config.yml or something similar
    config_paths: list[str] = [
        './config.yml',
        './config/config.yml',
        '/config/config.yml'
    ]

    found_config: bool = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            logger.debug("found config file at '%s'", config_path)
            builder.add_yaml(config_path)
            found_config = True

    if not found_config:
        raise FileNotFoundError("a suitable config.yml was not found!")

    cfg = {}

    cfg['mqtt'] = {}
    cfg['mqtt']['host'] = builder.get_key('mqtt.host')
    cfg['mqtt']['username'] = builder.get_key('mqtt.user')
    cfg['mqtt']['password'] = builder.get_key('mqtt.password')

    cfg['uhppoted'] = {}
    cfg['uhppoted']['mqtt_topic_root'] = \
        builder.get_key('uhppoted.mqtt_topic_root', DEFAULT_UHPPOTED_MQTT_TOPIC_ROOT)

    cfg['membership_portal'] = {}
    cfg['membership_portal']['url_get_tokens_list'] = \
        builder.get_key('membership_portal.url_get_tokens_list')
    cfg['membership_portal']['url_put_token_events'] = \
        builder.get_key('membership_portal.url_put_token_events')

    cfg['mqtt'] = {}
    cfg['mqtt']['host'] = builder.get_key('mqtt.host')
    cfg['mqtt']['username'] = builder.get_key('mqtt.username')
    cfg['mqtt']['password'] = builder.get_key('mqtt.password')

    cfg['uhppoted'] = {}
    cfg['uhppoted']['client_id'] = builder.get_key('uhppoted.client_id', 'idac')
    cfg['uhppoted']['devices'] = []

    # break out the configuration of devices!
    devices: Union[list, None] = builder.get_key('uhppoted.devices')
    for device_label, device in devices.items():
        device['label'] = device.get('label', device_label)
        cfg['uhppoted']['devices'].append(device)

    config = from_dict(Configuration, cfg)
    return config


DataclassType = TypeVar("DataclassType")


def from_dict(dataclass_type: Type[DataclassType], dictionary: dict[str, Any]) -> DataclassType:
    field_values = {}
    for name, value in dictionary.items():
        field_type = dataclass_type.__annotations__.get(name, None)

        # Check if the field is a dataclass
        if field_type and is_dataclass(field_type):
            value = from_dict(field_type, value)

        # Check if the field is a list
        elif field_type and hasattr(field_type, '__origin__') and isinstance(field_type.__origin__, type) and issubclass(field_type.__origin__, list):
            element_type = field_type.__args__[0]
            if is_dataclass(element_type):
                value = [from_dict(element_type, v) for v in value]

        field_values[name] = value

    return dataclass_type(**field_values)
