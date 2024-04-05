#%%

from array import array
import reprlib
import math 
import operator

class Vector:
    typecode = 'd'

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
    
    def __eq__(self, other):
        return tuple(self) == tuple(other)
    
    def __abs__(self):
        return math.hypot(*self)
    
    def __bool__(self):
        return bool(abs(self))
    
    def __len__(self):
        return len(self._components)
    
    def __getitem__(self,key):
        if isinstance(key,slice):
            cls = type(self)
            return cls(self._components[key])
        index = operator.index(key)
        return self._components[index]
    
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

# %%
