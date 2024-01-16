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


# %%

