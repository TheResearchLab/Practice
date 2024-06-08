#%%

class DoppleDict(dict):
    def __setitem__(self,key,value):
        super().__setitem__(key,[value] *2)

dd = DoppleDict(one=1)
dd
dd['two'] = 2
dd
dd.update(three=3)
dd

class AnswerDict(dict):
    def __getitem__(self,key):
        return 42
    
ad = AnswerDict(a='foo')
ad['a'] # returns 42
d = {}
d.update(ad)
d['a'] # foo is returned insted of returning 42
d # the answerdict __getitem__ is being ignored

# using collection
import collections

class DoppleDict2(collections.UserDict):
    def __setitem__(self,key,value):
        super().__setitem__(key,[value]*2)

dd = DoppleDict2(one=1)
dd # {'one':[1,1]}
dd["two"] = 2 
dd # {'one': [1, 1], 'two': [2, 2]}
dd.update(three=3)
dd

class AnswerDict2(collections.UserDict):
    def __getitem__(self,key):
        return 42
    
ad = AnswerDict2(a='foo')
ad['a'] # returns 42

d = {}
d.update(ad)
d['a']
d # works as expected with UserDict

# The Problem with Multiple Inheritance (Diamond Problem)

class Root:
    def ping(self):
        print(f'{self}.ping() in Root')
    
    def pong(self):
        print(f'{self}.pong in Root')

    def __repr__(self):
        cls_name = type(self).__name__
        return f'instance of {cls_name}'

class A(Root):
    def ping(self):
        print(f'{self}.ping() in A')
        super().ping()
    
    def pong(self):
        print(f'{self}.pong in A')
        super().ping()

class B(Root):
    def ping(self):
        print(f'{self}.ping() in B')
        super().ping()
    
    def pong(self):
        print(f'{self}.pong in B')
        super().ping()

class Leaf(A,B):
    def ping(self):
        print(f'{self}.ping in Leaf')
        super().ping()

leaf1 = Leaf()
leaf1.ping()
leaf1.pong()

# MRO
Leaf.__mro__ # shows the resolution, my 3.11 experience is different from the book

class U():
    def ping(self):
        print(f'{self}.ping() in U')
        super().ping()

class LeafUA(U,A):
    def ping(self):
        print(f'{self}.ping() in LeafUA')
        super().ping()

u = U()
#u.ping() # errors
leaf2 = LeafUA()
leaf2.ping()

# Mixin Classes - Case Insensitive Mappings

def _upper(key):
    try:
        return key.upper()
    except AttributeError:
        return key
    
class UpperCaseMixin:
    def __setitem__(self, key, item):
        super().__setitem__(_upper(key),item)
    
    def __getitem__(self,key):
        super().__getitem__(_upper(key))
    
    def get(self, key, default=None):
        return super().get(_upper(key),default)
    
    def __contains__(self,key):
        return super().__contains__(_upper(key))


class UpperDict(UpperCaseMixin,collections.UserDict):
    pass

class UpperCounter(UpperCaseMixin,collections.Counter):
    """ Specialized 'Counter' that uppercases string keys"""

d = UpperDict([('a','letter A'),(2,['digit two'])])
list(d.keys())
d['b'] = 'letter B'
'b' in d # true 



# %%
