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

# Grab excess Items

a,b,*c = range(5)
c # returns 2,3,4

a,*b,c = range(7)
a # return 0
c # returns 6
b # returns 1-5 

# UNPACKING WITH * IN FUNCTION AND SEQUENCE LITERALS

def func(a,b,c,*rest):
    return a,b,c,rest

output = func(*range(7))
output

# NESTED UNPACKING

locations = [
    ('Tokyo', 'JP', 36.933, (35.689722, 139.691667)),
    ('Delhi NCR', 'IN', 21.935, (28.613889, 77.208889)),
    ('Mexico City', 'MX', 20.142, (19.433333, -99.133333)),
    ('New York-Newark', 'US', 20.104, (40.808611, -74.020386)),
    ('São Paulo', 'BR', 19.649, (-23.547778, -46.635833)),
]

def nested_unpacker():
    for name,_,_,(lat,lon) in locations:
        if lon > 0:
            print(f'{name:15}|{lat:9.4f}|{lon:9.4f}')

nested_unpacker()

var1 = ((4),) # if you forget this then the worst error message is populated
type(var1)

# PATTERN MATCHING WITH SEQUENCES

def pattern_matcher(message):
    match message:
        case ['BEEPEER',frequency,times]:
            print(f'Beeping at {frequency} for {times} times.')
        case ['LED',computer]:
            print('my led {computer} is my new favorite')
        case [name,*extra,(lat,lon)]:
            print(f'{name} at {lat,lon} and all this other stuff {extra} (first)')
        case [str(name),*_,(lat,lon)]:
            print(f'{name} at {lat,lon} (second)')
        case [name,_,_,(lat,lon) as coord]:
            print(f'{name} at {coord}')
        case _:
            print('invalid message')


# pattern_matcher(['BEEPEER','23hz',4])
# pattern_matcher('hi mom')
# pattern_matcher(('Tokyo', 'JP', 36.933, (35.689722, 139.691667)))

# SLICING
s = 'bicycle'
s[::-1]
s[::3]

# ASSIGNING TO SLICES
l = list(range(10))
l[0::2] = [4,4,4,4,4] # returns an error unless all values explicitly given
del l[0::2] # delete specific values
l

s = [1,2,3] * 5
s

[[1]] * 4

# BUILDING A LIST OF LIST
ttt_board = [['_'] * 3 for i in range(3)] # list comprehension is the way to go
ttt_board[2][0] = 'X'
ttt_board

weird_board=[['_'] * 3] * 3
weird_board=[2][0] = 'X'
weird_board

# AUGMENTED ASSIGNMENTS WITH SEQUENCES
l = [3,2,1]
print(id(l))
l+=l
print(id(l)) # same reference
i = (3,2,1)
print(id(i))
i+=i
print(id(i)) # different reference


# A+=ASSIGNMENT PUZZLER
t=(1,2,[20,30])
#t[2] += [40,50] # tuple object does not support item assignment

t[2][0] = 3
t

# list.sort VERSUS THE SORTED BUILT-IN (pg 57)
lst = [9,4,1,6,7]
print(id(lst))
lst_sorted_func = sorted(lst)
lst.sort() # maintains id after sorting
print(lst.sort()) # returns None
print(lst)
print(id(lst))

fruits = ['apple','banana','grape','raspberry','melon','strawberry']
sorted(fruits,key=len,reverse=True)

# ARRAYS 
from array import array
from random import random 
# floats = array('d',(random() for i in range(10**7))) # typecode 'd' double-precison float
# floats[-1]


# fp = open('floats.bin','wb')
# floats.tofile(fp)
# fp.close()

# floats2 = array('d')
# fp = open('floats.bin','rb')
# floats2.fromfile(fp,10**7)
# fp.close()
# floats2[-1]
# floats == floats
# #floats.sort() # this is not available as of python 3.10

# # should rebuild the array using sorted instead
# floats = array(floats.typecode,sorted(floats))
# len(floats)

# MEMORY VIEWS 

# octets = array('B',range(6))
# m1 = memoryview(octets)
# m1.tolist()
# m2 = m1.cast('B',[3,2])
# m3 = m1.cast('B',[2,3])
# m2.tolist()
# m3.tolist()
# m2[1,1] = 25
# m2.tolist()

# numbers = array('h',list(range(-2,3,1)))
# memv = memoryview(numbers)
# memv[0]
# memv_oct = memv.cast('B')
# memv_oct.tolist()
# memv_oct[5] = 22
# numbers

# Arrays 
# import numpy as np 
# a = np.arange(12)
# a
# a.shape
# a.shape = 3,4 # reshape the array by changing the object shape property
# a
# a[2] 

# a[:,1] # grabs the column at index 1

# a.transpose() # this is sweet 

# DEQUE AND OTHER QUEUES

from collections import deque 
dq = deque(range(10),maxlen=10)
dq
dq.rotate(2)
dq.rotate(-2)
dq
dq.rotate(-4)
dq.rotate(4)
dq.extend([11,22,33]) # pops off values from ends
dq 
dq.extendleft([10,20,30,40]) # extends to the front of the deque data structure
dq

# CHAPTER SUMMARY
hash([]) # great way to validate
# %%

