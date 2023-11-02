from configuration import get_config, Configuration
from idac import MembershipPortalClient, LegacyToken, LegacyTokenStatus
from rfid.redbee import redbee_hex_str_to_wiegand34
from schema import UhppotedRequest, UhppotedReply, Card
from literals import (
    DEFAULT_CONTROLLER_COMM_TIMEOUT_SECS,
    DEFAULT_CONTROLLER_REQUEST_REPLY_TIMEOUT
)

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
# from aiomqtt.client import Message, Topic

# establish a logger here in the possibility this is used as as module
logger = logging.getLogger()


class UhppoteController:

    device_id: str
    mqtt_topic_root: str
    publish_queue: asyncio.Queue
    reply_queue: asyncio.Queue
    last_request_id: int
    last_heard_time: float
    request_reply_timeout: float

    cards: list[int]

    def __init__(self, device_id: str, publish_queue: asyncio.Queue, mqtt_topic_root: str):
        self.device_id = device_id
        self.mqtt_topic_root = mqtt_topic_root
        self.last_request_id = 0
        self.publish_queue = publish_queue
        self.reply_queue = asyncio.Queue()
        self.cards = []
        self.last_heard_time = time()
        self.request_reply_timeout = DEFAULT_CONTROLLER_REQUEST_REPLY_TIMEOUT

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
                        'device-id': self.device_id
                    }
                }
            }
        )
        self.last_request_id += 1
        return request

    async def delete_card(self, card_number: int) -> None:
        # Deletes a card from a controller
        topic = f"{self.mqtt_topic_root}/requests/device/card:delete"
        request = self._build_request(topic)
        request.payload['message']['request']['card-number'] = card_number
        await self.publish_queue.put(request)

        # wait for response!!

    async def delete_cards(self) -> None:
        # Deletes all cards from a controller
        topic = f"{self.mqtt_topic_root}/requests/device/cards:delete"
        await self.publish_queue.put(self._build_request(topic))

        reply: UhppotedReply = await asyncio.wait_for(
            self.reply_queue.get(), self.request_reply_timeout)

        if reply.response.get('deleted', False) is not True:
            raise Exception("unexpected response from delete_cards!")

        logger.info("delete_cards() successful")

    async def get_status(self) -> None:
        topic = f"{self.mqtt_topic_root}/requests/device/status:get"
        await self.publish_queue.put(self._build_request(topic))

        reply: UhppotedReply = await self.reply_queue.get()
        self.process_status_reply(reply)
        logger.info("controller reported status")

    def process_status_reply(self, reply: UhppotedReply) -> None:
        pass

    async def get_cards(self) -> list[int]:
        topic = f"{self.mqtt_topic_root}/requests/device/cards:get"
        await self.publish_queue.put(self._build_request(topic))

        reply: UhppotedReply = await asyncio.wait_for(
            self.reply_queue.get(), self.request_reply_timeout)

        # this is just a simple list of integers -- not full dicts
        self.cards = reply.response['cards']

        return self.cards

    async def get_card(self, card_number: int) -> dict:
        # Retrieves a card record from a controller
        topic = f"{self.mqtt_topic_root}/requests/device/card:get"
        request = self._build_request(topic)
        request.payload['message']['request']['card-number'] = card_number
        await self.publish_queue.put(request)

        reply: UhppotedReply = await asyncio.wait_for(
            self.reply_queue.get(), self.request_reply_timeout)

        logger.debug("get_card() response: %s", reply.response)

        # need to drill down further
        return reply.response

    async def put_card(self, card_number: int, doors: list[int]) -> None:
        # adds or updates a card record on a controller
        topic = f"{self.mqtt_topic_root}/requests/device/card:put"
        request = self._build_request(topic)
        request['message']['request']['card-number'] = card_number
        # default to all doors right now!
        await self.publish_queue.put(request)

        reply: UhppotedReply = await asyncio.wait_for(
            self.reply_queue.get(), self.request_reply_timeout)

        if reply.response.get('card-number') != card_number:
            raise Exception("put_card(%s) error! response %s",
                            card_number, reply.response)

    #   "response": {
    #         "device-id": "<controller-id>",
    #         "card": "record",
    #         "card-number": "uint32",
    #         "start-date": "date",
    #         "end-date": "date",
    #         "doors": "{1:uint8, 2:uint8, 3:uint8, 4:uint8}",
    #   },

    async def process_reply(self, reply: UhppotedReply) -> None:
        self.last_heard_time = time()  # crude for now
        await self.reply_queue.put(reply)

    @property
    def comms_timedout(self) -> bool:
        return (time() - self.last_heard_time) < DEFAULT_CONTROLLER_COMM_TIMEOUT_SECS


class IdacUhppotedAdapter:

    _is_running: bool  # property 'self.is_running'
    _mqtt_connected: bool  # property 'self.mqtt_connected'
    config: Configuration
    client_id: str
    devices: dict[str, UhppoteController]
    last_heard_time: float
    portal: MembershipPortalClient
    stopped: bool
    mqtt_publish_queue: asyncio.Queue
    cards: dict[str, Card]

    def __init__(self, config: Configuration) -> None:

        self.config = config

        self.client_id = 'idac'

        self._is_running = True
        self._mqtt_connected = False
        self.last_heard_time = 0

        self.cards = {}

        if len(self.config.uhppoted.devices) == 0:
            raise Exception("no UHPPOTE devices configured!")

        self.mqtt_publish_queue = asyncio.Queue()

        # configure helper classes for the controllers
        self.devices = {}
        for device in self.config.uhppoted.devices:
            self.devices[device.device_id] = UhppoteController(
                device_id=device.device_id,
                mqtt_topic_root=self.config.uhppoted.mqtt_topic_root,
                publish_queue=self.mqtt_publish_queue)

        self.portal = MembershipPortalClient(
            url_get_tokens_list=self.config.membership_portal.url_get_tokens_list,
            url_put_token_events=self.config.membership_portal.url_put_token_events)

    def process_portal_legacy_tokens_list(self, tokens: list[LegacyToken]) -> None:

        for portal_token in tokens:
            try:
                card = Card(
                    code=redbee_hex_str_to_wiegand34(portal_token.code))

                # Enable/Disable Card
                if portal_token.status == LegacyTokenStatus.ENABLED:
                    card.valid = True

                # determine if change
                if card.code in self.cards:
                    if card.valid is not self.cards[card.code].valid:
                        logger.debug("card '%s' changed valid: '%s' -> '%s'",
                                     self.cards[card.code].valid, card.valid)

                # force anyways
                self.cards[card.code] = card

            except Exception as e:
                logger.error("unable to process token: %s [%s]", portal_token, e)

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

                    # cancel connection since the tasks are done
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
        reply_topic = f"{self.config.uhppoted.mqtt_topic_root}/replies/{self.client_id}"
        async with client.messages() as messages:
            async for message in messages:

                # associate/route replies against the particular controller/request
                if message.topic.matches(reply_topic):
                    try:
                        m: dict = json.loads(message.payload.decode())

                        method: str = m['message']['reply']['method']

                        if 'response' not in m['message']['reply']:
                            raise Exception("reply missing response! %s", m)

                        response: dict = m['message']['reply']['response']

                        request_id: str = m['message']['reply'].get('request-id')

                        logger.debug(f"response to '{method}': {response}")

                        device_id: str = response.get('device-id')
                        if device_id not in self.devices:
                            raise Exception(f"device_id '{device_id}'"
                                            "does not match a known device")

                        reply = UhppotedReply(method=method,
                                              device_id=device_id,
                                              response=response,
                                              request_id=request_id)

                        await self.devices[device_id].process_reply(reply)

                    except Exception as e:
                        logger.error(f"{e}")

                else:
                    logger.debug("received an unmatched message: '%s' -> '%s'",
                                 message.topic, message.payload.decode())

    async def mqtt_producer(self, client: AsyncMqttClient) -> None:
        while True:
            request: UhppotedRequest = await self.mqtt_publish_queue.get()
            topic: str = request.topic
            request.payload['message']['request']['client-id'] = self.client_id
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

        # wait for mqtt
        while not self.mqtt_connected:
            logger.debug("waiting for mqtt to indicate it is connected")
            await asyncio.sleep(1)

        for controller in self.devices.values():
            await controller.get_status()

        # for controller in self.devices.values():
        #     await controller.delete_cards()

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

    @property
    def valid_cards(self) -> int:
        num_valid: int = 0
        for card in self.cards.values():
            if card.valid:
                num_valid += 1
        return num_valid


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
