#%%

from array import array
import reprlib
import math 
import operator
import functools
import itertools

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
    
    # def __eq__(self, other):
    #    return tuple(self) == tuple(other) # inefficient, creates copies and can be bad for large mutlidim vectors

    # def __eq__(self,other):
    #     if len(self) != len(other)
    #         return False 
    #     for a,b in zip(self,other):
    #         if a != b:
    #             return False
    #     return True # this implementation is effecient but can be done in one line

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
    
    @classmethod
    def frombytes(cls, octets):
        typecode = chr(octets[0])
        memv = memoryview(octets[1:]).cast(typecode)
        return cls(memv)
    

    
# PROTOCOL AND DUCK TYPING
    
import collections

Card = collections.namedtuple('Card',['rank','suit'])

class FrenchDeck:
    ranks = [str(n) for n in range(2,11)] + list('JQKA')
    suits = 'spades diamonds clubs hearts'.split()

    def __init__(self):
        self._cards = [Card(rank,suit) for suit in self.suits 
                                       for rank in self.ranks]
    
    def __len__(self):
        return len(self._cards)
    
    def __getitem__(self,position):
        return self._cards[position]
    


v1 = Vector([3,4,5])
len(v1)
v1[0],v1[-1]

v7 = Vector(range(7))
v7[1:4] # this would be better as a Vector slice instead of creating an array... losing functionality

# HOW SLICING WORKS

class MySeq:
    def __getitem__(self,index):
        return index
    
s = MySeq()
s[1]
s[1:4]
s[1:4:2]
s[1:4:2,9]
s[1:4:2,7:9]

slice

dir(slice)
help(slice.indices)

slice(None,10,2).indices(5) # (0,5,2)
slice(-3,None,None).indices(5) # (3,5,1)

'ABCDE'[:10:2] == 'ABCDE'[0:5:2] # True
'ABCDE'[-3:] == 'ABCDE'[2:5:1] # True



# After implementing better getitem
v7 = Vector(range(7))
v7[-1]
v7[1:4]
v7[-1:] # all returns vector slice now


# HASHING AND A FASTER == 

# calculating accum xor of integers 0 to 5
# method 1

n = 0
for i in range(1,6):
    n ^= i 
n 

# method 2
functools.reduce(lambda a,b: a^b,range(6))

# method 3
functools.reduce(operator.xor, range(6))


# %%
