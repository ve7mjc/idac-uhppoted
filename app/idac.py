import requests

from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class TokenStatus(Enum):
    DISABLED = 1
    ENABLED = 2
    LOST = 3
    AUTO_DISABLED = 4


@dataclass
class Token:
    id: int
    code: str
    status: TokenStatus
    member_id: int
    member_label: str


@dataclass
class TokensList:
    tokens: list[Token] = field(default_factory=list)


def process_v1_token_list(content: str) -> list[Token]:

    # data,token_id,member_id,token_status,token_label,null,null

    tokens: list[Token] = []

    expected_cols: int = 7
    for record in content.splitlines():
        parts = record.split(',')
        if len(parts) != expected_cols:
            logger.warning("warning! record has '%s' parts: %s",
                           len(parts), record)
        token = Token(
            code=parts[0],
            id=int(parts[1]),
            member_id=parts[2],
            status=TokenStatus(int(parts[3])),
            member_label=parts[4]
        )
        tokens.append(token)

    return tokens


class MembershipPortalClient:

    url_get_tokens_list: str
    url_put_token_events: str

    def __init__(self, url_get_tokens_list: str,
                 url_put_token_events: str) -> None:

        self.url_get_tokens_list = url_get_tokens_list
        self.url_put_token_events = url_put_token_events

    def get_tokens_list(self) -> TokensList:

        try:
            response = requests.get(self.url_get_tokens_list)
            response.raise_for_status()

            with open("./cache/response.txt", "w") as file:
                file.write(response.text)

            tokens: list[Token] = process_v1_token_list(response.text)

            logger.info(f"retrieved and processed {len(tokens)} tokens!")

            return tokens

        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")  # Python 3.6+
        except Exception as err:
            print(f"An error occurred: {err}")
        return {}
