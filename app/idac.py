import aiohttp
from aiohttp import ClientResponseError, ClientConnectorError  # , ClientError
from asyncio import TimeoutError
from dataclasses import dataclass, field
from enum import Enum
import logging

from rfid.redbee import legal_redbee_code

logger = logging.getLogger(__name__)


class LegacyTokenStatus(Enum):
    DISABLED = 1
    ENABLED = 2
    LOST = 3
    AUTO_DISABLED = 4


@dataclass
class LegacyToken:
    id: int
    code: str
    status: LegacyTokenStatus
    member_id: int
    member_label: str


@dataclass
class TokensList:
    tokens: list[LegacyToken] = field(default_factory=list)


def process_v1_token_list(content: str) -> list[LegacyToken]:

    # Legacy (v1) token list format:
    # data,token_id,member_id,token_status,token_label,null,null\r\n

    tokens: list[LegacyToken] = []

    expected_cols: int = 7
    for record in content.splitlines():
        parts = record.split(',')
        if len(parts) != expected_cols:
            logger.warning("warning! record has '%s' parts: %s",
                           len(parts), record)

        token = LegacyToken(
            code=parts[0],
            id=int(parts[1]),
            member_id=parts[2],
            status=LegacyTokenStatus(int(parts[3])),
            member_label=parts[4]
        )

        # we are going to drop tokens that have been mutilated in the database
        # for some reason ('', 'xxx', 'xxxxxx')
        if legal_redbee_code(token.code):
            tokens.append(token)

    return tokens


class MembershipPortalClient:

    url_get_tokens_list: str
    url_put_token_events: str

    def __init__(self, url_get_tokens_list: str,
                 url_put_token_events: str) -> None:

        self.url_get_tokens_list = url_get_tokens_list
        self.url_put_token_events = url_put_token_events

    async def get_tokens_list(self) -> TokensList:
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.url_get_tokens_list) as response:
                    response_data = await response.read()
                    response_text = response_data.decode('utf-8')
                    return process_v1_token_list(response_text)

        except ClientResponseError as e:
            # server returned an error response (e.g., 404, 500, etc.)
            logger.error("HTTP Status Error: %s", e.status)
            raise e
        except ClientConnectorError as e:
            # connection to server failed
            logger.error("Connection Error: %s", e)
            raise e
        except TimeoutError:
            # The request timed out
            logger.error("The request timed out")
            raise TimeoutError
        except Exception as e:
            # Catch any other exceptions that were not anticipated
            logger.error("Unexpected error: %s", e)
            raise e
