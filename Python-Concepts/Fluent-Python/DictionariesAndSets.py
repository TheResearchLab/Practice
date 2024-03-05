#%%
# MODERN DICT SYNTAX
dial_codes = [(880,'Bangladesh'),(55,'Brazil'),(86,'China'),(91,'India')]

country_dial = {code:country for code,country in dial_codes}
country_dial

# UNPACKING MAPPINGS **

def dump(**kwargs):
    return kwargs

dump(**{'c':234},y=4,**{'a':2345})

# MERGE MAPPINGS WITH |

d1 = {'me':'aaron','myself':'yo','i':'him'}
d2 = {'me':'charlie','it':'tu','you':'haha'}

print(d1 |d2) 
d1|=d2  # in-place mapping update
d1

# PATTERN MATCHING WITH MAPPINGS
def get_maker(record: dict) -> list:
    match record:
        case {'type':'book','api':2,'authors':[*names]}:
            return names
        case {'type':'book','api':1,'author':[name]}:
            return [name]
        case {'type':'book'}: # needs the above key to operate, allowed to have more as well
            return ValueError("Invalid 'book' record: {record!r}") 
        case {'type':'movie','director':name}:
            return [name]
        case _:
            raise ValueError(f'Invalid record: {record!r}')
        
bible_dict = {'title':'bible', 'type':'book','api':2,'authors':['father','son','holy spirit']}

get_maker(bible_dict)



# capture the remaining keys in a dictionary and return as a dictionary?
def get_remainder(record:dict) -> dict:
    match record:
        case {'name':'him','age':33,**details}:
            return f'details about him {details}' # returns as a dictionary
        
himothy = {'name':'him','age':33,'favorite food':'bread','favorite drink':'wine'} # Note 33 is different from '33'

get_remainder(himothy)

# STANDARD API OF MAPPING TYPES
tt = (1,2,(33,44))
hash(tt)

tt = [1,2,3,4,5,5]
#hash(tt) #returns error

tf = (1,2,frozenset([10,20]))
hash(tf)

# The __missing__ Method

class StrKeyDict0(dict):
    def __missing__(self,key):
        if isinstance(key,str):
            raise KeyError(key)
        return self[str(key)]
        
    def get(self,key,default=None):
        try:
            return self[key]
        except:
            return default
        
    def __contains__(self,key):
        return key in self.keys() or str(key) in self.keys()


# Collections.ChainMap
    
d1 = dict(a=1,b=2)
d2 = dict(a=2, b=4, c=6)

from collections import ChainMap
chain = ChainMap(d1,d2)
chain['a'] # retreives value from first dict
chain['a'] = 5
d1 # updating the chain map only impacts the first dictionary

# COLLECTIONS.COUNTER
from collections import Counter 

cnt_dict = Counter('aabbccdefggghijklmnopppqrstuuvwxyzz')
cnt_dict.most_common(2) # gets counts and returns the most common

# USERDICT vs DICT
import collections
class StrKeyDict(collections.UserDict):
    def __missing__(self,key):
        if isinstance(key,str):
            raise KeyError(key)
        return self[str(key)]
    
    def __contains__(self,key):
        return str(key) in self.data
    
    def __setitem__(self,key, item):
        self.data[str(key)] = item 

# IMMUTABLE MAPPINGS

from types import MappingProxyType

d = {1:'A'}
d_proxy = MappingProxyType(d)

d_proxy # instance of mappingproxy type

#d_proxy[2] = 'A' # this errors because mapping proxy types are read only
d[2] = 'A'
d_proxy # however it will still show the changes of the underlying mapping

# DICTIONARY VIEWS
#values_class = type({}.values())
#v = values_class() # cannot create 'dict_values' instances, it is a dynamic proxy

# SET THEORY
l = ['apple','apple','orange','pear','peach','peach']
list(dict.fromkeys(l).keys()) # deduplicates while preserving the order.


# SET LITERALS 

# create an empty set
s = set()
type(s)

# create a populated set
s = {1,2,3} # can't do s = {} to create an empty set. Will only create a dict
type(s)

# creating a frozen set
fs = frozenset(range(10))
fs

# SET COMPREHENSIONS
l = list(range(9)) + list(range(9))
sc = {num for num in l}
sc # creates a set from list with duplicates

# SET OPERATIONS
a = range(2,10,2)
b = range(0,4,1)
c = range(12,24,2)
d = range(100,39,-4)

s = {*a,*b,*c,*d} # creates a set from 4 iterables
s 

# complement of the intersection
set(a) ^ set(b)


# SET OPERATIONS ON DICT VIEWS
d1 = dict(a=1,b=2,c=3,d=4)
d2 = dict(b=20,d=40,e=50)
d1.keys() & d2.keys()

s = {'a','e','w'}
d1.keys() & s 
# %%
