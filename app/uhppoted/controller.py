from .schema import ControllerState, UhppotedRequest, UhppotedReply, UhppotedCard
from literals import (
    DEFAULT_CONTROLLER_REQUEST_REPLY_TIMEOUT,
    DEFAULT_CONTROLLER_COMM_TIMEOUT_SECS
)

import asyncio
import logging
from time import time
from typing import Optional


logger = logging.getLogger(__name__)


class UhppoteController:
    device_id: str
    mqtt_topic_root: str
    last_request_id: int
    request_reply_timeout: float
    state: ControllerState
    publish_queue: asyncio.Queue
    reply_queue: asyncio.Queue

    def __init__(self, device_id: str, publish_queue: asyncio.Queue, mqtt_topic_root: str):
        self.device_id = device_id
        self.mqtt_topic_root = mqtt_topic_root

        self.state = ControllerState()

        self.last_request_id = 0
        self.publish_queue = publish_queue
        self.reply_queue = asyncio.Queue()
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

    #
    def set_valid_response(self, method_type: Optional[str] = None) -> None:
        self.state.last_valid_time = time()

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

        self.set_valid_response()

        self.state.cards = {}  # state is known - albeit empty dict

        logger.info("delete_cards() successful")

    async def get_status(self) -> None:
        topic = f"{self.mqtt_topic_root}/requests/device/status:get"
        await self.publish_queue.put(self._build_request(topic))

        reply: UhppotedReply = await self.reply_queue.get()

        if reply.method != 'get-status':
            raise Exception("incorrect reply!")

        try:
            status = reply.response['status']
            self.set_valid_response()
            self._process_status_reply(status)

        except Exception:
            raise Exception("invalid response: %s", reply.response)

    def _process_status_reply(self, status: dict) -> None:
        pass

    async def get_cards(self) -> list[int]:
        topic = f"{self.mqtt_topic_root}/requests/device/cards:get"
        await self.publish_queue.put(self._build_request(topic))

        reply: UhppotedReply = await asyncio.wait_for(
            self.reply_queue.get(), self.request_reply_timeout)

        try:
            # ensure that we treat a valid response of no cards as known state
            if not isinstance(reply.response.get('cards'), list):
                raise Exception("unexpected response to get_cards(): %s",
                                reply.response)
        except Exception as e:
            raise e

        self.set_valid_response()

        self.state.cards = {}  # proceed as we have a valid cards list response
        for card in reply.response['cards']:
            self.state.cards[card] = {'code': card}

        cards_str: str = ', '.join(str(i) for i in self.state.cards)
        logger.debug(f"controller cards ({len(self.state.cards)}): {cards_str}")

        return self.state.cards

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

    async def put_card(self, card_number: int, doors: list[int] = []) -> None:
        # adds or updates a card record on a controller
        topic = f"{self.mqtt_topic_root}/requests/device/card:put"
        request = self._build_request(topic)
        request.payload['message']['request']['card'] = {
            'card-number': card_number,
            'doors': {'1': True, '2': True, '3': True, '4': True},
            "start-date": "2021-01-01",
            "end-date": "2029-12-31"
        }
        await self.publish_queue.put(request)

        reply: UhppotedReply = await asyncio.wait_for(
            self.reply_queue.get(), self.request_reply_timeout)

        if 'card' not in reply.response or (
                reply.response['card'].get('card-number') != card_number):
            raise Exception("put_card(%s) error! response %s",
                            card_number, reply.response)

        self.set_valid_response()

        #

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

    async def sync_cards(self, cards: dict[int, UhppotedCard]) -> None:
        if self.cards is None:  # cannot sync unless we know our cards
            return

        for code, card in cards.items():
            if code not in self.cards:
                if not card.valid:
                    continue  # card is not valid
                logger.debug("adding card %s", code)
                await self.put_card(code)

    # @property
    # def comms_timedout(self) -> bool:

    @property
    def healthy(self) -> bool:
        if self.state.last_valid_time is None:
            return False  # never heard from
        timeout_secs = DEFAULT_CONTROLLER_COMM_TIMEOUT_SECS
        return (time() - self.state.last_valid_time) < timeout_secs
