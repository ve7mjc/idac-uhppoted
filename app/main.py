from config import get_config, Configuration
from schema import Request

# from typing import Optional
from dataclasses import asdict
import logging
from time import sleep
import json

import paho.mqtt.client as mqtt


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.basicConfig()


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

    def __init__(self, config: Configuration) -> None:

        self.config = config

        if len(self.config.uhppoted.devices) == 0:
            raise Exception("no UHPPOTED devices configured!")

        self.last_request = 0

        self.mqttc = mqtt.Client()

        self.mqttc.username_pw_set(self.config.mqtt.username,
                                   self.config.mqtt.password)

        self.mqttc.on_connect = self.on_mqtt_connect
        self.mqttc.on_message = self.on_mqtt_message

        self.mqttc.connect(self.config.mqtt.host, self.config.mqtt.port, 60)

        self.mqttc.loop_start()

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
        return Request(
            request_id=request_id,
            client_id=self.config.uhppoted.client_id,
            device_id=self.config.uhppoted.devices[0].device_id,
        )


if __name__ == '__main__':

    config = get_config()
    idac = IdacAdapter(config)
