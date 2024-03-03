#%% 

# TREATING FUNCTION LIKE A OBJECT
def factorial(n):
    return 1 if n<2 else n * factorial(n-1)
fact = factorial
arr = [1,2,3,4,5,6,7,8,9]
list(map(fact,arr))

fruits = ['strawberry', 'fig', 'apple', 'cherry', 'raspberry', 'banana']
sorted(fruits,key=len)

sorted(fruits,key=lambda word: word[::-1]) # anonymous function used in filter

# THE NINE FLAVORS OF CALLABLE OBJECTS

callable(fact) # this built-in function determines if callable

# USER-DEFINED CALLABLE TYPES

import random 

class BingoCage:

    def __init__(self,items):
        self._items = list(items)
        random.shuffle(self._items)
    
    def pick(self):
        try:
            return self._items.pop()
        except IndexError:
            raise LookupError('pick from empty BingoCage')

    def __call__(self):
        return self.pick()
    

game_1 = BingoCage([1,2,3,4,5,6])
game_1()

# FROM POSITIONAL TO KEYWORD-ONLY PARAMETERS

def f(*,a,b):
    return a + b

#f(1,2) # weird type error message, takes 0 positional arguments
f(a=1,b=2) # force keyword arguments only

# POSITIONAL-ONLY PARAMETERS
def divmod(a,b,/):
    return (a//b,a%b)

divmod(11,2) # this works
#divmod(a=11,b=2) # this does not because / positional only argument


# PACKAGE FOR FUNCTION PROGRAMMING

from functools import reduce

def factorial(n):
    return reduce(lambda a,b: a*b, range(1,n+1))


from operator import mul 
def factorial(n):
    return reduce(mul,range(1,n+1))


metro_data = [('Tokyo', 'JP', 36.933, (35.689722, 139.691667)),
('Delhi NCR', 'IN', 21.935, (28.613889, 77.208889)),
('Mexico City', 'MX', 20.142, (19.433333, -99.133333)),
('New York-Newark', 'US', 20.104, (40.808611, -74.020386)),
('São Paulo', 'BR', 19.649, (-23.547778, -46.635833)),
]

from operator import itemgetter
for city in sorted(metro_data,key=itemgetter(1)):
    print(city) # prints tuple by the country code


cc_name = itemgetter(1,0)
for city in metro_data:
    print(cc_name(city))
# %%
