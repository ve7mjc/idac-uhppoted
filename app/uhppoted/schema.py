from typing import Optional
from dataclasses import dataclass
import asyncio


class ControllerException(Exception):
    pass


class ControllerTimeout(ControllerException):
    pass


@dataclass
class UhppotedCard:
    code: int
    valid: bool = False


@dataclass
class UhppotedRequest:
    method: str
    device_id: str
    topic: str
    payload: dict
    request_id: Optional[int] = None
    future: Optional[asyncio.Future] = None


@dataclass
class UhppotedReply:
    method: str
    device_id: str
    response: dict
    request_id: Optional[int] = None


@dataclass
class ControllerState:
    known: Optional[bool] = False
    last_valid_time: Optional[float] = None
    cards: Optional[dict[int, UhppotedCard]] = None
