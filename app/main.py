from configuration import get_config, Configuration
from idac import MembershipPortalClient
from schema import Request

# from typing import Optional
from dataclasses import asdict
import logging
from logging.handlers import RotatingFileHandler
from time import sleep
import os
import json

import paho.mqtt.client as mqtt

# establish a logger here in the possibility this is used as as module
logger = logging.getLogger()

# MQTT Return Codes and their description
MQTT_RC: dict[int, str] = {
    0: "Connection successful",
    1: "Connection refused - incorrect protocol version",
    2: "Connection refused - invalid client identifier",
    3: "Connection refused - server unavailable",
    4: "Connection refused - bad username or password",
    5: "Connection refused - not authorized"
}


class UhppotedClient:
    pass


class IdacAdapter:

    last_request: int
    mqttc: mqtt.Client

    config: Configuration

    portal: MembershipPortalClient

    def __init__(self, config: Configuration) -> None:

        self.config = config

        if len(self.config.uhppoted.devices) == 0:
            raise Exception("no UHPPOTE devices configured!")

        self.portal = MembershipPortalClient(
            url_get_tokens_list=self.config.membership_portal.url_get_tokens_list,
            url_put_token_events=self.config.membership_portal.url_put_token_events)

        self.last_request = 0

        self.mqttc = mqtt.Client()

        self.mqttc.username_pw_set(self.config.mqtt.username,
                                   self.config.mqtt.password)

        self.mqttc.on_connect = self.on_mqtt_connect
        self.mqttc.on_message = self.on_mqtt_message

        self.mqttc.connect(self.config.mqtt.host, self.config.mqtt.port, 60)

        self.mqttc.loop_start()

        tokens_list = self.portal.get_tokens_list()

        for token in tokens_list:
            print(token)

        # run for 5 seconds then peace out
        sleep(5)
        self.mqttc.loop_stop()

    def on_mqtt_connect(self, client, userdata, flags, rc):

        if rc != 0:
            logger.error("error connecting to mqtt %s@%s:%s - %s",
                         self.config.mqtt.username, self.config.mqtt.host,
                         self.config.mqtt.port, {MQTT_RC.get(rc, 'Unknown code')})

            return

        logger.info("connected to mqtt broker %s:%s",
                    self.config.mqtt.host, self.config.mqtt.port)

        self.mqttc.subscribe(f"{self.config.uhppoted.mqtt_topic_root}/events/#")
        self.mqttc.subscribe(f"{self.config.uhppoted.mqtt_topic_root}/replies/#")

        self.send_request(self.generate_request())

    def on_mqtt_message(self, client, userdata, msg):
        print(f"Received message '{msg.payload.decode()}' on topic '{msg.topic}'")

    def send_request(self, request: Request) -> None:
        req_topic = f"{self.config.uhppoted.mqtt_topic_root}/requests/device/status:get"
        # req = Request(device_id="425035705", client_id="halocal")
        self.mqttc.publish(req_topic, json.dumps(asdict(request)).encode())
        logger.debug(f"sent request: topic = '{req_topic}'\n request = {request}")
        self.last_request += 1
        # request sent

    def generate_request(self) -> Request:
        request_id = f'idac-r{self.last_request + 1}'
        # TODO: only looking at first device right now
        return Request(
            request_id=request_id,
            client_id=self.config.uhppoted.client_id,
            device_id=self.config.uhppoted.devices[0].device_id,
        )


if __name__ == '__main__':

    #
    # Configure logging
    #

    logs_path = os.environ.get("LOGS_PATH", "./logs")
    if not os.path.exists(logs_path):
        os.mkdir(logs_path)

    log_file = os.path.join(logs_path, 'idac-uhppote.log')

    # 5 MB per file, keep 5 old copies
    file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 5,
                                       backupCount=5)

    console_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # set levels
    logger.setLevel(logging.DEBUG)
    console_handler.setLevel(logging.WARNING)
    file_handler.setLevel(logging.DEBUG)

    try:
        config = get_config()
    except FileNotFoundError:
        print("cannot locate config file!")
        sleep(120)
        exit(1)

    idac = IdacAdapter(config)
