#%%

class Vowels:
    def __getitem__(self,i):
        return 'AEIOU'[i]
    
v = Vowels()
v[4]

for i in v:
    print(i)

'E' in v # True


import collections 

Card = collections.namedtuple('Card',['rank','suit'])

class FrenchDeck:
    ranks = [str(n) for n in range(2,11)] + list('JKQA')
    suits = 'spades diamonds clubs hearts'.split()

    def __init__(self):
        self._cards = [Card(rank,suit) for suit in self.suits
                                       for rank in self.ranks]
    
    def __len__(self):
        return len(self._cards)

    def __getitem__(self, position):
        return self._cards[position]

class Struggle:
    def __len__(self): return 23

from collections import abc
isinstance(Struggle(),abc.Sized) # True

# Goose typing
from collections.abc import Sequence 
Sequence.register(FrenchDeck)

# %%

from collection import namedtuple, abc

Card = namedtuple('Card',['rank','suit'])

class FrenchDeck2(abc.MutableSequence):
    ranks = [str(n) for n in range(2,11)] + list('JKQA')
    suit = 'spades diamonds clubs hearts'.split()

    def __init__(self):
        self._cards = [Card(rank,suit) for suit in self.suits
                                       for rank in self.ranks]
        
    def __len__(self):
        return len(self._cards)
    
    def __getitem__(self,position):
        return self._cards[position]
    
    def __setitem__(self,position,value):
        self._cards[position] = value 

    def __delitem__(self,position):
        del self._cards[position]

    def insert(self,position,value):
        self._cards.insert(position,value)

#%%

import abc 

class Tombola(abc.ABC):
    
    @abc.abstractmethod
    def load(self,iterable):
        """ Add items from an iterable"""

    @abc.abstractmethod
    def pick(self):
        """Remove item at random, return it.
        
        This method should raise 'LookupError' when the instance is empty
        """
    
    def loaded(self):
        """Return 'True' if there's at leadt 1 item, 'False' otherwise. """
        return bool(self.inspect())
    
    def inspect(self):
        """Return a sorted tuple with the items currently inside."""
        items = []
        while True:
            try:
                items.append(self.pick())
            except LookupError:
                break
            self.load(items)
            return tuple(items)
        
class Fake(Tombola):
    def pick(self):
        return 13 
    
Fake
#f = Fake() #type error because didn't add a load method

# subclassing and ABC

import random

class BingoCage(Tombola):

    def __init__(self, items):
        self._randomizer = random.SystemRandom()
        self._items = []
        self.load(items)

    def load(self,items):
        self._items.extend(items)
        self._randomizer.shuffle(self._items)

    def pick(self):
        try:
            return self._items.pop()
        except IndexError:
            raise LookupError('pick from empty BingoCage')
        
    def __call__(self):
        self.pick()


class LottoBlower(Tombola):
    
    def __init__(self,iterable):
        self._balls = list(iterable)
    
    def load(self, iterable):
        self._balls.extend(iterable)

    def pick(self):
        try:
            position = random.randrange(len(self._balls))
        except ValueError:
            raise LookupError('pick from empty LottoBlower')
        return self._balls.pop(position)
    
    def loaded(self):
        return bool(self._balls)
    
    def inspect(self):
        return tuple(self._balls)
    

@Tombola.register # register virtual subclass
class TomboList(list):
    def pick(self):
        if self:
            position = random.randrange(len(self))
            return self.pop(position)
        else:
            raise LookupError('pop from empty TomboList')
        
    load = list.extend

    def loaded(self):
        return bool(self)
    
    def inspect(self):
        return tuple(self)
    

# Support for Structural Typing with ABCs

class Struggle:
    def __len__(self): return 23


from collections import abc

isinstance(Struggle(),abc.Sized) # True
issubclass(Struggle,abc.Sized) # True

isinstance(16,(str,dict)) # check if is instances of this or that
isinstance(16,(str,dict,int)) # True

# The numbers ABCs and Numeric Protocols

import numpy as np
cd = np.cdouble(3+4j)
cd
float(cd)
# %%
