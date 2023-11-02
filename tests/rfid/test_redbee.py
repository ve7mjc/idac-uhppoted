from unittest import TestCase

from app.rfid.redbee import redbee_hex_str_to_wiegand34


class TestRedbee(TestCase):

    def setUp(self) -> None:
        pass

    def test_redbee_hex_str_to_wiegand34_int(self) -> None:

        # note mixture of uppercase/lowercase HEX chars
        valid_redbee_tag: str = "0F5424120e"
        wiegand34_expected: int = 2377840

        wiegand34_out = redbee_hex_str_to_wiegand34(valid_redbee_tag)

        self.assertEqual(wiegand34_out, wiegand34_expected)

        # ensure response was int
        self.assertIsInstance(wiegand34_out, int)

        # test hex string < 10 chars
        redbee_tag_short: str = "05424120e"
        with self.assertRaises(ValueError):
            redbee_hex_str_to_wiegand34(redbee_tag_short)

        # test hex string > 10 chars
        redbee_tag_long: str = "05424120ef2"
        with self.assertRaises(ValueError):
            redbee_hex_str_to_wiegand34(redbee_tag_long)

        # test invalid hex (non 0x00 - 0xff)
        redbee_tag_illegal_hex: str = "fg00112233"
        with self.assertRaises(ValueError):
            redbee_hex_str_to_wiegand34(redbee_tag_illegal_hex)
