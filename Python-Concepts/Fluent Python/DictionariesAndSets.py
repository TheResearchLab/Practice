#%%
# MODERN DICT SYNTAX
dial_codes = [(880,'Bangladesh'),(55,'Brazil'),(86,'China'),(91,'India')]

country_dial = {code:country for code,country in dial_codes}
country_dial

# UNPACKING MAPPINGS **

def dump(**kwargs):
    return kwargs

dump(**{'c':234},y=4,**{'a':2345})

d1 = {'me':'aaron','myself':'yo','i':'him'}
d2 = {'me':'charlie','it':'tu','you':'haha'}

d1 |d2 
d1|=d2 
d1

# %%
