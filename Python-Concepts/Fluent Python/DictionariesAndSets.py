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

# INSERTING OR UPDATING MUTABLE VALUES


# %%
