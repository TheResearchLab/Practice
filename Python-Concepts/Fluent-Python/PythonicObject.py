#%%
from array import array
import math

class Vector2d:
    typecode = 'd'

    def __init__(self,x,y):
        self.x = float(x) 
        self.y = float(y)

    def __iter__(self):
        return (i for i in (self.x,self.y))

    
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
        components = (format(c,fmt_spec) for c in self)
        return '({},{})'.format(*components)
    
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




# %%
