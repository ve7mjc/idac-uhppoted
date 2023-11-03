from typing import Optional
from dataclasses import dataclass


@dataclass
class UhppotedCard:
    code: int
    valid: bool = False


@dataclass
class UhppotedRequest:
    device_id: str
    topic: str
    payload: dict
    request_id: Optional[int] = None


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
