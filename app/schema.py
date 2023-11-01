from dataclasses import dataclass


@dataclass
class Request:
    request_id: str
    client_id: str
    device_id: str
