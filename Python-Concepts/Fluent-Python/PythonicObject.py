#%%
from array import array
import math

class Vector2d:
    typecode = 'd'
    __match_args__ = ('x','y')

    def __init__(self,x,y):
        self.__x = float(x) 
        self.__y = float(y)

    @property
    def x(self):
        return self.__x 
    
    @property
    def y(self):
        return self.__y
    
    def __iter__(self):
        return (i for i in (self.x,self.y))

    def __hash__(self):
        return hash((self.x,self.y))
    
    def __repr__(self):
        class_name = type(self).__name__
        return '{}({!r},{!r})'.format(class_name, *self)
    
    def __str__(self):
        return str(tuple(self))
    
    def __bytes__(self):
        return (bytes([ord(self.typecode)]) +
                bytes(array(self.typecode,self)))
    
    def __eq__(self, other):
        return tuple(self) == tuple(other)
    
    def __abs__(self):
        return math.hypot(self.x,self.y)
    
    def __bool__(self):
        return bool(abs(self))
    
    def __format__(self,fmt_spec=''):
        if fmt_spec.endswith('p'):
            fmt_spec = fmt_spec[:-1]
            coords = (abs(self),self.angle())
            outer_fmt = '<{},{}>'
        else:
            coords = self
            outer_fmt = '({}, {})'
        components = (format(c,fmt_spec) for c in coords)
        return outer_fmt.format(*components)
    
    def angle(self):
        return math.atan2(self.x,self.y)
    
    @classmethod
    def frombytes(cls, octets):
        typecode = chr(octets[0])
        memv = memoryview(octets[1:]).cast(typecode)
        return memv    


# Class Demo

class Demo:
    @classmethod
    def klassmeth(*args):
        return args
    @staticmethod
    def statmeth(*args):
        return args 
    
Demo.klassmeth('spam') # returns all positional args
Demo.klassmeth() # demo class is always the first method

Demo.statmeth('spam') # behaves like a function



# FORMATTED DISPLAYS

brl = 1/4.82 

format(brl,'0.4f')
'1 BRL = {rate:0.2f} USD'.format(rate=brl)
f'1 USD = {1/brl:0.2f} BRL'

# can support binary
format(42,'b')

# or percentages
format(2/3,'.1%') # percent with one decimal place

v1 = Vector2d(2,3)
format(v1) # this works due to having a string method

# however this will not work because lacks format method
# format(v1,'.3f') # returns a type error

# WENT BACK AND ADDED FORMAT METHOD TO CLASS

format(Vector2d(1,1),'p')
format(Vector2d(1,1),'.3ep') # need to understand format options
format(Vector2d(1,1),'0.5fp')

# A HASHABLE VECTOR2D

v1 = Vector2d(3,4)
#hash(v1) # unhashable warning
#set([v1]) # another one

#v1.x = 5 # returns attribute error because property decorator

# after hashable update

v1 = Vector2d(3,4)
v2 = Vector2d(3.1,4.1)
{v1,v2}

# SUPPORTING POSITIONAL PATTERN MATCHING

def keyword_pattern_demo(v:Vector2d) -> None:
    match v:
        case Vector2d(x=0,y=0):
            print(f'{v!r} is null')
        case Vector2d(x=0):
            print(f'{v!r} is vertical')
        case Vector2d(y=0):
            print(f'{v!r} is horizontal')
        case Vector2d(x=x,y=y) if x==y: # interesting
            print(f'{v!r} is diagonal')
        case _:
            print(f'{v!r} is awesome')
        




def positional_pattern_demo(v:Vector2d) -> None:
    match v:
        case Vector2d(0,0):
            print(f'{v!r} is null')
        case Vector2d(_,0):
            print(f'{v!r} is also horizontal')
        case Vector2d(0):
            print(f'{v!r} this one is vertical')
        case Vector2d(x=x,y=y) if x==y:
            print(f'{v!r} these are somehow diagonal')
        case _:
            print(f'{v!r} is awesome')
        

keyword_pattern_demo(Vector2d(0,7))
positional_pattern_demo(Vector2d(0,7))



# %%
