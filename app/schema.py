from dataclasses import dataclass
from typing import Optional


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
class Card:
    code: int
    valid: bool = False
