"""

RoboticsConnection "RedBee" RFID Reader support library

Note: The RedBee RFID reader is no longer produced but tags captured from it
      require special handling since the data is in a non-standard format

The RedBee RFID readers supports EM41xx 125 kHz RFID tags and supports
a direct USB connection and a wireless XBee serial interface.

EM41xx 125 kHz tag scan results in an output of 5 bytes, the format of which is
fairly obscure.

{0-255} {0-255} {0-255} {0-255} {0-255}
eg.
123 0 66 43 21\n

The decimal represented bytes are also bit mirrored

"""

from .utils import hex_string_to_bytes, bit_mirror_bytes, em41xx_to_wiegand34_int


def redbee_code_fix(redbee_tag: list[bytes]) -> list[bytes]:

    if len(redbee_tag) != 5:
        raise ValueError("expected redbee tag code of 5 bytes! got %s",
                         len(redbee_tag))

    return bit_mirror_bytes(redbee_tag)


def redbee_hex_str_to_wiegand34(redbee_str: str) -> int:

    if len(redbee_str) != 10:
        raise ValueError('redbee tag code is incorrect length! (%s)')

    em41xx = redbee_code_fix(hex_string_to_bytes(redbee_str))

    return em41xx_to_wiegand34_int(em41xx)
