#%%

# OVERVIEW OF DATA CLASS BUILDERS

# class Point:
#     def __init__(self,x,y):
#         self.x = x
#         self.y = y 

# from collections import namedtuple 
# Coordinate = namedtuple('Coordinate','lat lon')
# issubclass(Coordinate, tuple)

# moscow = Coordinate(55.756,37.617)
# print(moscow)
# moscow == Coordinate(lat=55.756,lon=37.617) # True

# import typing

# Coordinate = ( 
#     typing.NamedTuple('Coordinate',
#                       [('lat',float),('lon',float)])
# )

# typing.get_type_hints(Coordinate) #pretty cool


import typing

class Coordinate(typing.NamedTuple):
    lat: float
    lon: float

    def __str__(self):
        ns = 'N' if self.lat >= 0 else 'S'
        we = 'E' if self.lon >= 0 else 'W'
        return f'{abs(self.lat):.1f}°{ns},{abs(self.lon):.1f}°{we}'




# NamedTuple is not actually the superclass
#issubclass(Coordinate,typing.NamedTuple)
issubclass(Coordinate,tuple)

from dataclasses import dataclass

@dataclass(frozen=True)
class Coordinate:
    lat: float
    lon: float

    def __str__(self):
        ns = 'N' if self.lat >= 0 else 'S'
        we = 'E' if self.lon >= 0 else 'W'
        return f'{abs(self.lat):.1f}°{ns},{abs(self.lon):.1f}°{we}'


issubclass(Coordinate,object) #dataclass is subclass of object

# CLASSIC NAMED TUPLES
from collections import namedtuple
City = namedtuple('City','name country population coordinates print')

Gotham = City('Gotham','US',10000000,(35,90),print)
Gotham.population
Gotham.coordinates
Gotham.print('Hello World') # wild

miami_data = ('Miami','US',1232454352,(100,100),print)
miami = City._make(miami_data)
miami.print('lol')
miami._asdict()

# %%
