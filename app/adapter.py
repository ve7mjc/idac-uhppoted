# project libs
from configuration import Configuration
from uhppoted.controller import UhppoteController
from uhppoted.schema import (
    UhppotedCard, UhppotedReply, UhppotedRequest,
    ControllerException  # , ControllerTimeout
)
from idac.portal import MembershipPortalClient, LegacyToken, LegacyTokenStatus
from rfid.redbee import redbee_hex_str_to_wiegand34
from schema import CardListChanges
from literals import (
    DEFAULT_PORTAL_CARDS_LIST_STALE_SECS
)

# system libs
import asyncio
from typing import Optional
from datetime import datetime
import logging
from time import time
import json

# third party libs
from aiomqtt import Client as AsyncMqttClient, MqttError


logger = logging.getLogger(__name__)


class IdacUhppotedAdapter():

    stop_event: asyncio.Event
    _mqtt_connected: bool  # property 'self.mqtt_connected'
    config: Configuration
    client_id: str
    devices: dict[str, UhppoteController]
    last_heard_time: float
    portal: MembershipPortalClient
    stopped: bool
    mqtt_publish_queue: asyncio.Queue
    _controller_sync_enabled: bool
    tasks: list[asyncio.Task]
    last_uhppoted_watchdog_alive: time
    last_uhppoted_health_check_alive: time

    # list of organizational cards (intention)
    cards: dict[str, UhppotedCard]
    cards_last_changed: Optional[datetime] = None
    cards_last_fetched: Optional[datetime] = None

    debug_methods: bool
    debug_ignore_methods: list[str]

    request_futures: dict[int, asyncio.Future]

    def __init__(self, config: Configuration) -> None:

        self.config = config

        self.client_id = 'idac'

        self.stop_event = asyncio.Event()
        self._controller_sync_enabled = False
        self._mqtt_connected = False
        self.last_heard_time = 0
        self.tasks = []

        # assume we have heard these for now -- we are more concerned
        # about their absence
        self.last_uhppoted_watchdog_alive = time()
        self.last_uhppoted_health_check_alive = time()

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

        self.cards = {}
        self.cards_last_changed = None
        self.cards_last_fetched = None

        self.portal = MembershipPortalClient(
            url_get_tokens_list=self.config.membership_portal.url_get_tokens_list,
            url_put_token_events=self.config.membership_portal.url_put_token_events)

        # associate replies to their awaiting requesters via futures
        self.request_futures = {}

        # debugging
        self.debug_methods: bool = True
        self.debug_ignore_methods = ['get-cards', 'get-status']

    def process_portal_legacy_tokens_list(self, legacy_tokens: list[LegacyToken]) -> None:

        local_empty: bool = len(self.cards) == 0 or False

        # assume a list of no cards is a no-go
        if len(legacy_tokens) == 0:
            raise Exception("received portal cards list is empty!")

        self.cards_last_fetched = time()

        changes = CardListChanges()

        # convert portal cards into local cards
        portal_cards: dict[str, UhppotedCard] = {}
        for legacy_token in legacy_tokens:
            try:
                wiegand34_code = redbee_hex_str_to_wiegand34(legacy_token.code)
                portal_cards[wiegand34_code] = UhppotedCard(
                    code=wiegand34_code,
                    valid=legacy_token.status == LegacyTokenStatus.ENABLED or False
                )
            except Exception as e:
                # note: a malformed portal token is going to get noisy over time
                logger.error("unable to process token: %s [%s]", legacy_token, e)

        # search for new cards aka cards we do not have locally
        for portal_card in portal_cards.values():
            if portal_card.code not in self.cards:
                changes.num_added += 1
            else:
                # check for enabled/disabled changes
                if portal_card.valid is not self.cards.get(portal_card.code).valid:
                    if portal_card.valid:
                        changes.num_enabled += 1
                    else:
                        changes.num_disabled += 1

            # force anyways
            self.cards[portal_card.code] = portal_card

        # search for cards no longer in the remote list
        cards_for_removal: list[int] = []
        for card_code in self.cards:
            if card_code not in portal_cards:
                cards_for_removal.append(card_code)
                changes.num_removed += 1
        for card in cards_for_removal:
            logger.debug(f'removing card {card}')
            del self.cards[card]

        # describe nature and magnitude of changes
        if changes.num_changes > 0 and not local_empty:
            c: list[str] = []
            if changes.num_added:
                c.append(f"added = {changes.num_added}")
            if changes.num_removed:
                c.append(f"removed = {changes.num_removed}")
            if changes.num_enabled:
                c.append(f"enabled = {changes.num_enabled}")
            if changes.num_disabled:
                c.append(f"disabled = {changes.num_disabled}")
            logger.info(f"card list changes: {', '.join(c)}")

        if local_empty:
            num_enabled: int = \
                sum(1 for c in portal_cards.values() if c.valid is True)
            logger.info("initial card list downloaded: %s cards (%s active)",
                        len(portal_cards), num_enabled)

        if changes.num_changes >= 0:
            self.cards_last_changed = time()

    async def _portal_token_list_getter_task(self):
        loop_time: float = self.config.membership_portal.list_poll_time_secs
        retry_time: float = 30
        while True:
            try:
                tokens: list[LegacyToken] = await self.portal.get_tokens_list()
            except Exception as e:
                logger.error("could not obtain cards list from portal: %s", e)
                await asyncio.sleep(retry_time)
                continue

            try:
                self.process_portal_legacy_tokens_list(tokens)
            except Exception as e:
                logger.error("could not process portal tokens list: %s", e)
                await asyncio.sleep(retry_time)
                continue

            try:
                await self.sync_controllers()
                await asyncio.sleep(loop_time)
            except Exception as e:
                logger.error("could not syncronize controllers: %s", e)
                await asyncio.sleep(retry_time)

    async def _mqtt_task(self):
        # mqtt_connect_timeout: float = 5
        invalid_connect_attempts: int = 0
        while not self.stop_event.is_set():
            connect_attempt_time: float = time()
            try:
                async with AsyncMqttClient(
                    hostname=self.config.mqtt.hostname,
                    port=self.config.mqtt.port,
                    username=self.config.mqtt.username,
                    password=self.config.mqtt.password,
                    timeout=60,
                    keepalive=15
                ) as client:

                    invalid_connect_attempts = 0  # reset our counter
                    self._mqtt_connected = True

                    logger.debug('mqtt connected to broker')

                    sub_topics: list[str] = [
                        f"{self.config.uhppoted.mqtt_topic_root}/events/#",
                        f"{self.config.uhppoted.mqtt_topic_root}/replies/#",
                        f"{self.config.uhppoted.mqtt_topic_root}/system/#"
                    ]
                    for sub_topic in sub_topics:
                        logger.debug("subscribed to topic: %s", sub_topic)
                        await client.subscribe(sub_topic)

                    # MQTT forever loop of checking and publishing messages
                    while not self.stop_event.is_set():
                        publisher = asyncio.create_task(self._mqtt_producer_task(client),
                                                        name="mqtt_producer")
                        receiver = asyncio.create_task(self._mqtt_consumer_task(client),
                                                       name="mqtt_consumer")
                        self.tasks.extend([publisher, receiver])
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
                    logger.debug(f"throttling connection attempts; sleeping for {delay_secs}")

                await asyncio.sleep(delay_secs)

    async def _mqtt_consumer_task(self, client: AsyncMqttClient) -> None:
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

                        if self.debug_methods and method not in self.debug_ignore_methods:
                            print(f"method '{method}' response:\n",
                                  json.dumps(m, indent=4))

                        device_id: str = response.get('device-id')
                        if device_id not in self.devices:
                            raise Exception(f"device_id '{device_id}'"
                                            "does not match a known device")

                        reply = UhppotedReply(method=method,
                                              device_id=device_id,
                                              response=response,
                                              request_id=request_id)

                        if request_id in self.request_futures:
                            future = self.request_futures[request_id]
                            if not future.done():
                                future.set_result(reply)

                    except Exception as e:
                        logger.error("exception processing received message: "
                                     "'%s', error=%s", message.payload.decode(), e)

                elif message.topic.matches(f"{self.config.uhppoted.mqtt_topic_root}/system"):
                    try:
                        msg: dict = json.loads(message.payload.decode())['message']['system']
                        if "alive" in msg:
                            subsystem: str = msg['alive']['subsystem']
                            if subsystem == "watchdog":
                                self.last_uhppoted_watchdog_alive = time()
                                logger.debug("uhppoted system watchdog: alive!")
                            elif subsystem == "health-check":
                                self.last_uhppoted_health_check_alive = time()
                                logger.debug("uhppoted system health-check: alive!")

                        elif "alert" in msg:
                            subsystem: str = msg['alert']['subsystem']
                            alert_msg: str = msg['alert']['message']

                            logger.warning("uhppoted system.%s alert: %s",
                                           subsystem, alert_msg)

                        else:
                            logger.warning("received unknown /system message: %s", msg)
                    except Exception as e:
                        logger.error("unable to process /system message: %s (%s)", msg, e)

                else:
                    logger.debug("received an unmatched message: '%s' -> '%s'",
                                 message.topic, message.payload.decode())

    async def _mqtt_producer_task(self, client: AsyncMqttClient) -> None:
        last_request_id: int = 0
        while not self.stop_event.is_set():
            request: UhppotedRequest = await self.mqtt_publish_queue.get()

            # manage the request id and asyncio.Future
            request.request_id = str(last_request_id + 1)
            last_request_id += 1
            if request.future is not None:
                self.request_futures[request.request_id] = request.future

            topic: str = request.topic
            request.payload['message']['request']['client-id'] = self.client_id
            request.payload['message']['request']['request-id'] = request.request_id
            payload: bytes = json.dumps(request.payload).encode()
            if self.debug_methods and request.method not in self.debug_ignore_methods:
                logger.debug("publishing '%s' -> '%s'", request.topic, payload)
                print(f"method '{request.method}' response:\n",
                      json.dumps(request.payload, indent=4))

            await client.publish(topic, payload)
            self.mqtt_publish_queue.task_done()

    # system health check
    async def _system_health_checker_task(self) -> None:
        remote_cards_list_unhealthy: bool = False
        controllers_unhealthy: list[int] = []
        while not self.stop_event.is_set():

            # check controllers
            for device in self.devices.values():
                if not device.healthy:
                    if device.device_id in controllers_unhealthy:
                        continue  # already known
                    logger.error(f"controller '{device.device_id}' UNHEALTHY!")
                    controllers_unhealthy.append(device.device_id)
                elif device.device_id in controllers_unhealthy:
                    controllers_unhealthy.remove(device.device_id)
                    logger.info(f"controller {device.device_id} is healthy!")

            if not self.is_portal_cards_valid:
                if not remote_cards_list_unhealthy:
                    logger.error("remote cards list is UNHEALTHY")
                    remote_cards_list_unhealthy = True
            elif remote_cards_list_unhealthy:
                logger.error("remote cards list is healthy")
                remote_cards_list_unhealthy = False

            await asyncio.sleep(0.01)

    async def _controller_status_poller_task(self) -> None:
        # be careful about firing off a request to a controller while
        # another task has already asked for it -- no locking yet
        while not self.stop_event.is_set():
            for device in self.devices.values():
                await device.get_status()

            await asyncio.sleep(15)

    async def sync_controllers(self) -> None:

        if not self.controller_sync_enabled:
            logger.warn("sync_controllers is disabled; ignoring")
            return

        # mirror list - merge maintainer cards
        cards_list: dict[int, UhppotedCard] = self.cards
        for m_label, m_card in self.config.maintainer_cards.items():
            if m_card not in cards_list or cards_list[m_card].valid is False:
                cards_list[m_card] = UhppotedCard(code=m_card, valid=True)

        # confirm prerequisite conditions
        if len(self.cards) == 0:
            return

        # validate org card list
        # device: UhppoteController
        for device in self.devices.values():
            await device.sync_cards(self.cards)

    #
    # ADAPTER START POINT
    #
    # Consider this the "light-off" point
    # We assume nothing as we know no states of concern:
    # - Controller State
    # - Organization UhppotedCard List State
    #
    async def start(self) -> None:

        self._controller_sync_enabled = True

        service_start_time: float = time()

        # begin the process of syncronization of controller and portal card list
        self.tasks = [
            asyncio.create_task(self._mqtt_task(), name="mqtt_client"),
            asyncio.create_task(self._portal_token_list_getter_task(),
                                name="token_list_getter")
        ]

        #
        # Wait on MQTT connection
        #
        warned_mqtt_connecting: bool = False
        while not self.mqtt_connected and not self.stop_event.is_set():
            if (time() - service_start_time) > 3 and not warned_mqtt_connecting:
                logger.warning("waiting for mqtt connection ...")
                warned_mqtt_connecting = True
            await asyncio.sleep(0.05)

        for controller in self.devices.values():
            try:
                await controller.get_status()
            except ControllerException:
                pass

        #
        # Wait on controllers to be considered healthy
        # - warn every 5 seconds
        #
        warned_controllers_healthy: float = time()
        waiting_on_controller: bool = True
        while waiting_on_controller and not self.stop_event.is_set():
            if (time() - warned_controllers_healthy) >= 5:
                logger.warning("waiting for controllers ...")
                warned_controllers_healthy = time()
            waiting_on_controller = False
            for device in self.devices.values():
                if not device.healthy:
                    waiting_on_controller = True
            await asyncio.sleep(0.05)

        #
        # Wait on remote portal cards list
        # warn every 5 seconds
        #
        warned_portal_list_waiting: float = time()
        while not self.is_portal_cards_valid and not self.stop_event.is_set():
            if (time() - warned_portal_list_waiting) >= 5:
                logger.warning("waiting for remote cards list ...")
                warned_portal_list_waiting = time()
            waiting_on_controller = False
            await asyncio.sleep(0.05)

        logger.info("system ready!")

        force_controller_refresh: bool = False
        if force_controller_refresh:
            for controller in self.devices.values():
                try:
                    await controller.delete_cards()
                except ControllerException as e:
                    logger.error("could not delete all cards! error =", e)

        for controller in self.devices.values():
            try:
                await controller.get_cards()
            except ControllerException as e:
                logger.error("could not get cards! error =", e)

        try:
            await self.sync_controllers()
        except Exception as e:
            logger.error("could not sync controllers! error =", e)

        #
        # We can start system health check now
        #
        self.tasks.extend([
            asyncio.create_task(self._system_health_checker_task(),
                                name="system_health_check"),
            asyncio.create_task(self._controller_status_poller_task(),
                                name="controller_status_poller")
        ])

        await asyncio.gather(*self.tasks)

    async def shutdown(self):
        logger.info("shutdown requested")
        self.stop_event.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    @property
    def controller_sync_enabled(self) -> bool:
        return self._controller_sync_enabled

    @property
    def mqtt_connected(self) -> bool:
        return self._mqtt_connected

    @property
    def is_portal_cards_valid(self) -> bool:
        """whether we have a validated list of cards from the remote portal
        Returns:
            bool: True if portal cards list is known and considered fresh
        """
        if self.cards_last_fetched is None:
            return False

        # let us just assume that no cards means no purpose for existence;
        # or, a problem, whichever comes first
        if len(self.cards) == 0:
            return False

        cards_list_age = time() - self.cards_last_fetched
        if cards_list_age >= DEFAULT_PORTAL_CARDS_LIST_STALE_SECS:
            return False

        return True

    @property
    def num_valid_cards(self) -> int:
        """number of valid cards in the cards list

        Returns:
            int: _description_
        """
        num_valid: int = 0
        for card in self.cards.values():
            if card.valid:
                num_valid += 1
        return num_valid
