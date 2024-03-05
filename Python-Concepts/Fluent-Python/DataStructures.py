#%%


################# A Pythonic Card Deck ################
import collections
from random import choice 


# Card = collections.namedtuple('Card',['rank','suit'])

# class FrenchDeck:
#     ranks = [str(n) for n in range(2,11)] + list('JKQA') #jack, king, queen, ace
#     suits = 'spades diamonds clubs hearts'.split()

#     def __init__(self):
#         self._cards = [Card(rank,suit) for suit in self.suits
#                                        for rank in self.ranks]
    
#     def __len__(self):
#         return len(self._cards)
    
#     def __getitem__(self,position):
#         return self._cards[position]
    
# beer_card = Card('7','diamonds')
# beer_card

# # Instantiate Class
# deck = FrenchDeck()
# len(deck)

# #get item
# deck[4]
# deck[0]

# # Pick a card any card
# choice(deck)# very cool

# # Slicing is supported 
# deck[0:4]
# deck[0::13]

# # can iterate over the class
# for card in deck:
#     print(card)

# #can also reverse the deck
# for card in reversed(deck):
#     print(card)
    
# # use "in" to iterate if not __contains__ is defined
# ('Q','diamonds') in deck # case sensitive still

# suit_values = dict(spades=3,hearts=2,diamonds=1,clubs=0)
# suit_values

# def spades_high(card):
#     rank_value = FrenchDeck.ranks.index(card.rank)
#     return rank_value * len(suit_values) + suit_values[card.suit]

# spades_high(deck[43])

# for card in sorted(deck,key=spades_high):
#     print(card)

################# How Special Methods Are Used ################
# for i in x: can initialize a call to __getiem__ or __iter__ depending on implementation in class
    

################# Emulating Numeric Types ################

import math

class Vector:
    def __init__(self,x=0,y=0):
        self.x = x
        self.y = y 
    
    def __repr__(self):
        return f'Vector({self.x!r},{self.y!r})'
    
    def __abs__(self):
        return math.hypot(self.x,self.y)
    
    def __bool__(self):
        return bool(abs(self))

    def __add__(self,other):
        x=self.x + other.x 
        y=self.y + other.y 
        return Vector(x,y)
    
    def __mul__(self,scalar):
        return Vector(self.x * scalar, self.y * scalar)
    
v1 = Vector(10,3)
v2 = Vector(1,1)

print(v1)
# %%
