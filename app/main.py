from configuration import get_config, Configuration
from idac import MembershipPortalClient, LegacyToken  # , LegacyTokenStatus
# from rfid.redbee import redbee_hex_str_to_wiegand34
from schema import UhppotedRequest

# from typing import Optional
import asyncio
# from dataclasses import asdict
import logging
from logging.handlers import RotatingFileHandler
from time import time
import os
import json

# import paho.mqtt.client as mqtt
from aiomqtt import Client as AsyncMqttClient, MqttError

# establish a logger here in the possibility this is used as as module
logger = logging.getLogger()


class UhppoteController:

    device_id: str
    mqtt_topic_root: str
    publish_queue: asyncio.Queue
    last_request_id: int

    def __init__(self, device_id: str, publish_queue: asyncio.Queue, mqtt_topic_root: str):
        self.device_id = device_id
        self.mqtt_topic_root = mqtt_topic_root
        self.last_request_id = 0
        self.publish_queue = publish_queue

    def _build_request(self, topic: str) -> UhppotedRequest:
        request_id = self.last_request_id + 1
        request = UhppotedRequest(
            request_id=request_id,
            device_id=self.device_id,
            topic=topic,
            payload={
                'message': {
                    'request': {
                        # 'request-id': request_id,
                        'client-id': 'idac',
                        # 'reply-to':
                        'device-id': self.device_id
                    }
                }
            }
        )
        self.last_request_id += 1
        return request

    async def get_status(self) -> None:
        topic = f"{self.mqtt_topic_root}/requests/device/status:get"
        await self.publish_queue.put(self._build_request(topic))

    async def get_cards(self) -> None:
        topic = f"{self.mqtt_topic_root}/requests/device/cards:get"
        await self.publish_queue.put(self._build_request(topic))

    async def get_card(self, card_number: int) -> None:
        topic = f"{self.mqtt_topic_root}/requests/device/cards:get"
        request = self._build_request(topic)
        request.payload['message']['request']['card-number'] = card_number
        await self.publish_queue.put(request)


class IdacUhppotedAdapter:

    _is_running: bool  # property 'self.is_running'
    _mqtt_connected: bool  # property 'self.mqtt_connected'

    config: Configuration

    controllers: dict[str, UhppoteController]

    portal: MembershipPortalClient

    stopped: bool

    mqtt_publish_queue: asyncio.Queue

    def __init__(self, config: Configuration) -> None:

        self.config = config

        self._is_running = True
        self._mqtt_connected = False

        if len(self.config.uhppoted.devices) == 0:
            raise Exception("no UHPPOTE devices configured!")

        self.mqtt_publish_queue = asyncio.Queue()

        # configure helper classes for the controllers
        self.controllers = {}
        for device in self.config.uhppoted.devices:
            self.controllers[device.device_id] = UhppoteController(
                device_id=device.device_id,
                mqtt_topic_root=self.config.uhppoted.mqtt_topic_root,
                publish_queue=self.mqtt_publish_queue)

        self.portal = MembershipPortalClient(
            url_get_tokens_list=self.config.membership_portal.url_get_tokens_list,
            url_put_token_events=self.config.membership_portal.url_put_token_events)

    def process_portal_legacy_tokens_list(self, tokens: list[LegacyToken]) -> None:
        pass
        # redbee_hex_str_to_wiegand34

    async def portal_token_list_getter_task(self):
        while True:
            logger.debug("requesting tokens list from portal")
            try:
                tokens: list[LegacyToken] = await self.portal.get_tokens_list()
                self.process_portal_legacy_tokens_list(tokens)
            except Exception as e:
                logger.info("unable to obtain tokens list: %s", e)

            await asyncio.sleep(10)

    async def _mqtt_task(self):
        # mqtt_connect_timeout: float = 5
        invalid_connect_attempts: int = 0
        while self.is_running:
            connect_attempt_time: float = time()
            try:
                async with AsyncMqttClient(
                    hostname=self.config.mqtt.hostname,
                    port=self.config.mqtt.port,
                    username=self.config.mqtt.username,
                    password=self.config.mqtt.password
                ) as client:

                    invalid_connect_attempts = 0  # reset our counter
                    self._mqtt_connected = True

                    logger.debug('mqtt connected to broker')

                    sub_topics: list[str] = [
                        f"{self.config.uhppoted.mqtt_topic_root}/events/#",
                        f"{self.config.uhppoted.mqtt_topic_root}/replies/#"
                    ]
                    for sub_topic in sub_topics:
                        logger.debug("subscribed to topic: %s", sub_topic)
                        await client.subscribe(sub_topic)

                    # MQTT forever loop of checking and publishing messages
                    while self.is_running:

                        publisher = asyncio.create_task(self.mqtt_producer(client))
                        receiver = asyncio.create_task(self.mqtt_consumer(client))
                        await asyncio.gather(publisher, receiver)

                        print("we are here")

                    # cancel connection
                    raise asyncio.CancelledError()

            except asyncio.CancelledError:
                # leaving the async with block triggers disconnection
                self._mqtt_connected = False
                logger.debug('disconnecting from mqtt broker')
                return

            except asyncio.TimeoutError:
                self._mqtt_connected = False
                invalid_connect_attempts += 1
                logger.error("mqtt connect timed out!")

            except MqttError as e:
                self._mqtt_connected = False
                invalid_connect_attempts += 1
                logger.error("mqtt error: %s", e)

                delay_secs: float = 0

                # scale delays up to 30 seconds for successive connection errors
                if invalid_connect_attempts > 2:
                    delay_secs = invalid_connect_attempts * 2
                    if delay_secs > 30:
                        delay_secs = 30

                # amnesty for time served
                if (time() - connect_attempt_time) > delay_secs:
                    delay_secs = 0

                if delay_secs:
                    print(f"throttling connection attempts; sleeping for {delay_secs}")

                await asyncio.sleep(delay_secs)

    async def mqtt_consumer(self, client: AsyncMqttClient) -> None:
        async with client.messages() as messages:
            async for message in messages:
                print(f"Received message on topic {message.topic}: {message.payload.decode()}")

    async def mqtt_producer(self, client: AsyncMqttClient) -> None:
        while True:
            request: UhppotedRequest = await self.mqtt_publish_queue.get()
            topic: str = request.topic
            payload: bytes = json.dumps(request.payload).encode()
            logger.debug("publishing '%s' -> '%s'", request.topic, payload)
            await client.publish(topic, payload)
            self.mqtt_publish_queue.task_done()

    async def start(self) -> None:
        self.tasks = [
            asyncio.create_task(self._mqtt_task()),
            # asyncio.create_task(self._periodic_status_request()),
            asyncio.create_task(self.portal_token_list_getter_task())
        ]

        for controller in self.controllers.values():
            await controller.get_cards()

        await asyncio.gather(*self.tasks)

    async def stop(self):
        self._is_running = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(self.tasks, return_exceptions=True)

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def mqtt_connected(self) -> bool:
        return self._mqtt_connected


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
        exit(1)

    idac = IdacUhppotedAdapter(config)
    asyncio.run(idac.start())
