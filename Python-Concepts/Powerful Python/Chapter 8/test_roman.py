import unittest
from romanNumerals import roman

class TestRoman(unittest.TestCase):
    def test_roman_error(self):
        with self.assertRaises(ValueError):
            roman('Nonsense Text')


