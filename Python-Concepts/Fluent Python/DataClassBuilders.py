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


# import typing

# class Coordinate(typing.NamedTuple):
#     lat: float
#     lon: float

#     def __str__(self):
#         ns = 'N' if self.lat >= 0 else 'S'
#         we = 'E' if self.lon >= 0 else 'W'
#         return f'{abs(self.lat):.1f}°{ns},{abs(self.lon):.1f}°{we}'




# NamedTuple is not actually the superclass
#issubclass(Coordinate,typing.NamedTuple)
# issubclass(Coordinate,tuple)

# from dataclasses import dataclass

# @dataclass(frozen=True)
# class Coordinate:
#     lat: float
#     lon: float

#     def __str__(self):
#         ns = 'N' if self.lat >= 0 else 'S'
#         we = 'E' if self.lon >= 0 else 'W'
#         return f'{abs(self.lat):.1f}°{ns},{abs(self.lon):.1f}°{we}'


# issubclass(Coordinate,object) #dataclass is subclass of object

# # CLASSIC NAMED TUPLES
# from collections import namedtuple
# City = namedtuple('City','name country population coordinates print')

# Gotham = City('Gotham','US',10000000,(35,90),print)
# Gotham.population
# Gotham.coordinates
# Gotham.print('Hello World') # wild

# miami_data = ('Miami','US',1232454352,(100,100),print)
# miami = City._make(miami_data)
# miami.print('lol')
# miami._asdict()


# City = namedtuple('City','name country population coordinates print founded')
# def get_age(self):
#     return 2024 - self.founded 

# City.age = get_age

# grand_rapids = City('GR','US',12345678,(150,100),print,1980)

# grand_rapids.age() # this is wild, can create methods outside of class and associated to namedTuple

# TYPED NAMED TUPLES
from typing import NamedTuple

class Coordinate(NamedTuple):
    lat: float
    lon: float
    reference: str = 'WGS84'

# TYPE HINTS 101

class Coordinate(NamedTuple):
    lat: float
    lon: float

trash = Coordinate('Not Float',None)
print(trash) # No runtime impact just nonsense


# The Meaning of Variable Annotations

class DemoPlainClass:
    a: int
    b: float = 1.1
    c = 'spam'

DemoPlainClass.__annotations__
#DemoPlainClass.a # no variable for A created just annotation

# Inspecting a typing.NamedTuple

class DemoNTClass(NamedTuple):
    a: int
    b: float = 1.1
    c = 'spam'

DemoNTClass.a # in namedtuple a variable placeholder is added


nt = DemoNTClass(5,2) # don't understand why attribute c can't be updated
print(nt.a,nt.b,nt.c)

# Inspecting a class decorated with dataclass

from dataclasses import dataclass

@dataclass
class DemoDataClass:
    a: int 
    b: float = 1.1 
    c = 'spam'

DemoDataClass.__annotations__
DemoDataClass.__doc__
#DemoDataClass.a # no attribute
DemoDataClass.b
DemoDataClass.c

dc = DemoDataClass(4)
dc.a
dc.b
dc.b = 12
dc.b
dc.k = 'blah'
dc.k # dataclass is mutable

# MORE ABOUT @DATACLASS

# @dataclass
# class ClubMember:
#     name: str 
#     guest: list = []

# errors because dataclass uses default values in type hints

from dataclasses import field

@dataclass
class ClubMember:
    name: str
    guest: list = field(default_factory=list)

@dataclass 
class ClubMember:
    name: str
    guests: list = field(default_factory=list)
    athlete: bool = field(default=False,repr=False) # default value to false and remove from repr method




# %%
