#%%
from collections import abc

issubclass(tuple,abc.Sequence) # verifies sequence
issubclass(list,abc.MutableSequence) # verifies mutable

# Local Scope in Comprehension and Generator Expression
x = 'ABC'
codes = [last:=ord(x) for x in x]
last

# list comprehensions are better
symbols =  '$¢£¥€¤'
beyond_ascii_1 = [ord(s) for s in symbols if ord(s) > 127]
# vs filters and maps
beyond_ascii_2 = list(filter(lambda c: c>127,map(ord,symbols)))

beyond_ascii_2

# speed test - https://github.com/fluentpython/example-code-2e/blob/master/02-array-seq/listcomp_speed.py

### Cartesian Products
colors = ['red','green','blue']
sizes = ['S','M','L']

inventory = [(color,size) for color in colors for size in sizes ] 
inventory

# generator expressions

inventory_2 = ((color,size) for color in colors for size in sizes)

# for i in inventory_2:
#     print(i)

# Tuples As Records
val1,val2,val3,val4 = ('this','that',1,2)
val1

# tuples reference are mutable but not objects being reference
tuple_1 = (2,3,[3,4])
tuple_1[-1].append(1)
tuple_1 # list object gets modified from within immutable tuple

def fixed(o):
    try:
        hash(o)
    except TypeError:
        return False 
    return True

#hash([1,2,3]) # mutable objects are not hashable


# Variable unpacking
a,b,c = 1,2,3
a

def do_things(a,b):
    print(a,b)

arr = ['Hi','Mom']
do_things(*arr)

# %%

