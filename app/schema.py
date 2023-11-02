from dataclasses import dataclass
from typing import Optional


@dataclass
class UhppotedRequest:
    device_id: str
    topic: str
    payload: dict
    request_id: Optional[int] = None
