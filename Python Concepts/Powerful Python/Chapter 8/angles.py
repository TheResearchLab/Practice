class Angle:
    def __init__(self,degrees):
        self.degrees = degrees%360
    def is_acute(self):
        return self.degrees < 90
    def add(self, other_angle):
        return Angle(self.degrees + other_angle.degrees)