
def hex_string_to_bytes(hex_string: str) -> list[int]:
    """
    Converts a string of hexadecimal values into a list of bytes.

    :param hex_string: String containing ASCII hex bytes (e.g., '00ff00').
    :return: List of integers representing the byte values.
    """
    # Ensure the hex string length is even
    if len(hex_string) % 2 != 0:
        raise ValueError("Hex string must have an even number of characters.")

    # Convert two characters at a time to a byte
    return [int(hex_string[i:i + 2], 16) for i in range(0, len(hex_string), 2)]


def bit_mirror_bytes(byte_list: list[int]) -> list[int]:
    """
    Reverses the bits in each byte of the list.

    :param byte_list: List of integer bytes.
    :return: A new list where each byte has its bits reversed.
    """
    mirrored_list = [
        int('{:08b}'.format(byte)[::-1], 2) for byte in byte_list
    ]
    return mirrored_list


def bytes_to_hex_string(byte_list: list[int]) -> str:
    """
    Converts a list of bytes to a hex string
    :param byte_list: List of integer bytes
    Returns:
        str: hex string; eg. "0fc0"
    """
    hex_string = ''.join(f'{byte:02x}' for byte in byte_list)
    return hex_string


def bytes_to_int(byte_list: list[int]) -> int:
    """
    Converts a list of bytes into an single integer. Python 'int' type can
    support integers of arbitrary bit-size and thus, there may be no practical
    limit to this.

    :param byte_list: list of bytes representing a big-endian unsigned integer
    :return: unsigned integer representing value of data in bytes
    """
    value: int = 0
    for byte in byte_list:
        value = (value << 8) | byte  # Shift and add the next byte
    return value


def int_to_binary_grouped(value: int) -> str:
    """convert integer (int) into a binary string

    Args:
        hex_string (str):

    Raises:
        ValueError: thrown if character count is not even

    Returns:
        str: grouped string of binary text eg: '00011010 11101010'
    """
    # Convert integer to binary without '0b' prefix, and ensure it's a multiple of 8 bits
    # Group into bytes (8 bits each)
    binary_str = bin(value)[2:].zfill(8 * ((len(bin(value)) - 2 + 7) // 8))
    return ' '.join(binary_str[i:i + 8] for i in range(0, len(binary_str), 8))


def hex_ascii_to_binary(hex_string: str) -> str:
    """_summary_

    Args:
        hex_string (str):

    Raises:
        ValueError: thrown if character count is not even

    Returns:
        str: grouped string of binary text eg: '00011010 11101010'
    """
    # Remove spaces and convert to uppercase
    hex_string = hex_string.replace(" ", "").upper()
    if len(hex_string) % 2 != 0:
        raise ValueError("Input string must have an even number of characters.")

    # Convert each hex pair to binary and group into 8 bits
    binary_groups = ' '.join(f"{int(hex_string[i:i+2], 16):08b}" for i in range(0, len(hex_string), 2))

    return binary_groups


def em41xx_to_wiegand34_int(em41xx: list[bytes]) -> int:
    """Convert a 5-byte EM41xx Tag Code to Wiegand34 integer.
    No Facility Code or User Code.

    Args:
        em41xx (list[bytes]): 5 bytes representing EM41xx tag scan

    Raises:
        ValueError: thrown if not supplied exactly 5 bytes

    Returns:
        int: value representing what a wiegand34-type reader would produce
             for a given em41xx tag scan
    """
    if len(em41xx) != 5:
        raise ValueError("expected EM41xx tag code of 5 bytes! got %s",
                         len(em41xx))

    return bytes_to_int(em41xx[2:5])
