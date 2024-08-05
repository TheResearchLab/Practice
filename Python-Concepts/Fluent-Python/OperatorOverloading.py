#%% 


principal = 4000 
rate = .08 
periods = 12 

interest = principal * ((1 + rate) ** periods - 1)
interest

# When x != +x 
# Change in context 
import decimal 
ctx = decimal.getcontext()
ctx.prec = 40 
one_third = decimal.Decimal('1') / decimal.Decimal('3')
display(one_third) 

one_third == +one_third
ctx.prec = 28 
print(one_third == +one_third) 
display(+one_third)
one_third

# counter example
from collections import Counter

ct = Counter('abracadabra')
ct
ct['b'] = -3 
ct['c'] = 0 
display(ct)
+ct # removes the non positive instances



### The Vector Class w/ new add and radd dunder methods

import itertools
from array import array
import reprlib
import functools
import operator
import math


class Vector:
    typecode = 'd'
    __match_args__ = ('x','y','z','t')

    def __init__(self,components):
        self._components = array(self.typecode,components)

    def __iter__(self):
        return iter(self._components)

    def __repr__(self):
        components = reprlib.repr(self._components)
        components = components[components.find('['):-1] # remove the typecode from the output
        return f'Vector({components})'
    
    def __str__(self):
        return str(tuple(self))
    
    def __bytes__(self):
        return (bytes([ord(self.typecode)]) + bytes(self._components))
    
    def __abs__(self):
        return math.hypot(*self)
    
    def __bool__(self):
        return bool(abs(self))
    
    def __len__(self):
        return len(self._components)
    

    def __eq__(self,other):
        return len(self) == len(other) and all(a==b for a,b in zip(self,other))

    
    def __hash__(self):
        hashes = map(hash,self._components)
        return functools.reduce(operator.xor, hashes)
    
    def __getitem__(self,key):
        if isinstance(key,slice):
            cls = type(self)
            return cls(self._components[key])
        index = operator.index(key)
        return self._components[index]

    def __setattr__(self,name,value):
        cls = type(self)
        if len(name) == 1:
            if name in cls.__match_args__:
                error = 'readonly attribute {attr_name!r}'
            elif name.islower():
                error = "can't set attributes 'a' to 'z' in {cls_name!r}"
            else:
                error = ''
            if error:
                msg = error.format(cls_name=cls.__name__, attr_name=name)
                raise AttributeError(msg)
        super().__setattr__(name,value) # if nothing else fails then set attr value
    
    def __getattr__(self,name):
        cls = type(self)
        try:
            pos = cls.__match_args__.index(name)
        except ValueError:
            pos = -1
        if 0 <= pos < len(self._components):
            return self._components[pos]
        msg = f'{cls.__name__!r} object has no attribute {name!r}'
        raise AttributeError(msg)
    
    def angle(self,n):
        r = math.hypot(*self[n:])
        a = math.atan2(r, self[n-1])
        if (n == len(self) -1) and (self[-1]<0):
            return math.pi * 2 - a
        else:
            return a 
    
    def angles(self,n):
        return (self.angle(n) for n in range(1,len(self)))
    
    def __format__(self, fmt_spec=""):
        if fmt_spec.endswith('h'):
            fmt_spec = fmt_spec[:-1]
            coords = itertools.chain([abs(self)],
                                     self.angles())
            outer_fmt = '<{}>'
        else:
            coords = self
            outer_fmt = '({})'
        components = (format(c,fmt_spec) for c in coords)
        return outer_fmt.format(', '.join(components))

    def __add__(self,other):
        try:
            pairs = itertools.zip_longest(self,other,fillvalue=0.0)
            return Vector(a + b for a,b in pairs)
        except TypeError:
            return NotImplemented
        
    def __radd__(self,other):
        return self + other
    
    #__radd__ = __add__
    
    def __mul__(self,scalar):
        try:
            factor = float(scalar)
        except TypeError:
            return NotImplemented
        return Vector(n * scalar for n in self)
        
    
    def __rmul__(self,scalar):
        return self * scalar

    @classmethod
    def frombytes(cls, octets):
        typecode = chr(octets[0])
        memv = memoryview(octets[1:]).cast(typecode)
        return cls(memv)



v1 = Vector([3,4,5])

v1 + 1 # doesn't work with non-iterable 
v1 + 'ABC' # doesn't work with non-float




# %%
