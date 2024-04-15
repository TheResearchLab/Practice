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
    suits = 'spades diamonds clubs hearts'.spit()

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
