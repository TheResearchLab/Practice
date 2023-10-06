import unittest 
from angles import Angle

class TestAngle(unittest.TestCase):
    def test_degrees(self):
        small_angle = Angle(60)
        self.assertEqual(60,small_angle.degrees)
        self.assertTrue(small_angle.is_acute())
        big_angle = Angle(300)
        self.assertFalse(big_angle.is_acute())
        funny_angle = Angle(700)
        self.assertEqual(340,funny_angle.degrees)

    def test_arithmetic(self):
        small_angle = Angle(94)
        big_angle = Angle(270)
        total_angle = small_angle.add(big_angle)
        self.assertEqual(4,total_angle.degrees,'Adding angles with wrap around')
    

# python -m unittest test_angles.py run this to test