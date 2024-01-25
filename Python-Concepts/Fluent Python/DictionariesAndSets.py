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

        

# %%
