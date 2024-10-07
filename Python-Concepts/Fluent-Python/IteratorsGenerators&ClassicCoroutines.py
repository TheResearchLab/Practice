#%% 

# Chapter 17

import re
import reprlib

RE_WORD = re.compile(r'\w+')

class Sentence:

    def __init__(self, text):
        self.text = text 
        self.words = RE_WORD.findall(text)
    
    def __getitem__(self,index):
        return self.words[index]
    
    def __len__(self):
        return len(self.words)
    
    def __repr__(self):
        return 'Sentence(%s)' % reprlib.repr(self.text)
    
s = Sentence('"The time has come", the Walrus said,')
s 
for word in s:
    print(word)
list(s)

from random import randint

def d6():
    return randint(1,6)

d6_iter = iter(d6,1)
d6_iter 

for roll in d6_iter:
    print(roll)

s = 'ABC' # iterable 

for letter in s: # hidden iterator
    print(letter)

# implementing the above with for loop


it = iter(s)
while True:
    try:
        print(next(it))
    except StopIteration:
        del it # garbage collection
        break


s3 = Sentence('Life of Aaron')
it = iter(s3)
it
next(it)
next(it)
next(it)
#next(it) # stop iteration
list(iter(s3))


from collections import abc

RE_WORD = re.compile('\w+')
class Sentence:
    
    def __init__(self,text):
        self.text = text 
        self.words = RE_WORD.findall(text)
    
    def __repr__(self):
        return f'Sentence({reprlib.repr(self.text)})'
    
    def __iter__(self):
        return SentenceIterator(self.words)


class SentenceIterator:

    def __init__(self,words):
        self.words = words 
        self.index = 0
    
    def __next__(self):
        try:
            word = self.words[self.index]
        except IndexError:
            raise StopIteration()
        self.index += 1
        return word 
    
    def __iter__(self):
        return self


issubclass(SentenceIterator,abc.Iterator) # passes check  because SI implements __iter__ and __next__

# Generator function Sentence class 

class Sentence:

    def __init__(self,text):
        self.text = text
        self.words = RE_WORD.findall(text)
    
    def __repr__(self):
        return 'Sentence(%s)' % reprlib.repr(self.text)

    def __iter__(self):
        for word in self.words:
            yield word # yield keywords produces a sentence class that is more idiomatic


def gen_123():
    yield 1
    yield 2 
    yield 3 

for i in gen_123():
    print(i)

g = gen_123()
next(g)
next(g)
next(g)



# Lazy Generator

class Sentence:

    def __init__(self,text):
        self.text = text 

    def __repr__(self):
        return f'Sentence({reprlib.repr(self.text)})'

    def __iter__(self):
        for match in RE_WORD.finditer(self.text):
            yield match.group() # generator function


def gen_AB():
    print('start')
    yield 'A'
    print('continue')
    yield 'B'
    print('end.')

res1 = [x*3 for x in gen_AB()]

for i in res1:
    print('-->',i)

res2 = (x*3 for x in gen_AB())
print(type(res2))

for i in res2:
    print('-->', i)


class Sentence:

    def __init__(self,text):
        self.text = text 

    def __repr__(self):
        return f"Sentence({reprlib.repr(self.text)})"
    
    def __iter__(self):
        return (match.group() for match in RE_WORD.finditer(self.text)) # generator expression
    

new_sentence = Sentence('This is the new sentence')

for word in new_sentence:
    print(word)

list(new_sentence)


class ArithmeticProgression:

    def __init__(self, begin, step, end=None):
        self.begin = begin 
        self.step = step 
        self.end = end # None -> infinite series

    def __iter__(self):
        result_type = type(self.begin + self.step)
        result = result_type(self.begin)
        forever = self.end is None 
        index = 0 
        while forever or result < self.end:
            yield result 
            index+=1 
            result = self.begin + self.step * index # avoid cumulative floating point problem 


100 * 1.1 # 110.0000000001
sum(1.1 for _ in range(100)) # 109.99999999999982

# Arithmetic Progression
import itertools 
gen = itertools.count(1,.5) # count will run forever. list(count()) would cause stackoverflow


for i in range(4):
    print(next(gen))

gen = itertools.takewhile(lambda n: n<3, itertools.count(1,.5))

list(gen) # this works because "takewhile" has a predicate that will fail before the call stack is full

def aritprog_gen(begin, step, end=None):
    first = type(begin + step)(begin)
    ap_gen = itertools.count(first,step)

    if end is None:
        return ap_gen 
    return itertools.takewhile(lambda n: n<end, ap_gen)

# filtering generator functions

def is_even(num):
    return num % 2 == 0

list(filter(is_even,[1,2,3,4,5])) # filter objects where predicate is false
next(filter(is_even,[1,2,3,4,5])) 

list(itertools.filterfalse(is_even,[1,2,3,4,5])) # filter objects when predicate is truthy
next(itertools.filterfalse(is_even,[1,2,3,4,5])) # still a generator

# mapping generators
sample =  [2,3,5,45,6,7,3,8,56,7,445,2,8,222]
list(itertools.accumulate(sample))
list(itertools.accumulate(sample,min)) # running minimum
list(itertools.accumulate(sample,max)) # running maximum

import operator 

list(itertools.accumulate(sample, operator.mul))
list(itertools.accumulate(range(1,11), operator.mul))

list(enumerate('HiMyNameIs',1)) # pair string with number index
list(map(operator.mul,range(11),range(11)))


sample = [5,4,2,8,7,6,3,0,9,1]
list(itertools.starmap(operator.mul,enumerate('albatroz',1)))
list(itertools.starmap(lambda a, b: b/a,
                       enumerate(itertools.accumulate(sample),1))) # running average

# Merging Generators

list(itertools.chain('ABC', range(2)))
list(itertools.chain(enumerate('ABC'))) # merge enumerate's index with ABC outputs a tuple
list(itertools.chain.from_iterable(enumerate('ABC'))) # same as above but outputs a list
list(zip('ABC',range(5),[10,20,30,40])) # tuples of 3 items that only go up to max length of the shortest input
list(itertools.zip_longest('ABC',range(5), fillvalue='?')) # add fillvalue to go the full range 5


# Cartesian products using itertools product generator
list(itertools.product('ABC',range(2)))
suits = 'spades hearts diamonds clubs'.split()
list(itertools.product('AKQ',suits)) 
list(itertools.product('ABC')) # doesn't add anything in the second position of the tuples within the list
list(itertools.product('ABC',repeat=3)) # this is crazy 
list(itertools.product('01',repeat=2)) # how is a variable passed into the method?

rows = itertools.product('AB',range(2),repeat=2)
for row in rows: print(row) # did not know this works either

# itertools count, cycle, pairwise, and repeat
ct = itertools.count()
next(ct) # can just use instead of creating your own counter
next(ct), next(ct), next(ct) # (1,2,3)

list(itertools.islice(itertools.count(1,.5),4)) # islice?

cy = itertools.cycle('ABC')
list(itertools.islice(cy,5)) # cycle around, wonder how it remembers which index it's on?

list(itertools.pairwise(range(7))) # creates an array sequence of tuples that pairs

rp = itertools.repeat(7)
next(rp), next(rp) 

list(itertools.repeat(8,4))


list(map(operator.mul, range(11),itertools.repeat(5))) # interesting example of mapping to a constant for multiplication. Each number in range is paired to a 5

# Combinatorics Generators

list(itertools.combinations('ABC',2)) # 3 options choose 2 unique chars
list(itertools.combinations_with_replacement('ABC',2)) # 3 option choose 2 can pick same twice
list(itertools.permutations('ABC',2)) # find all the unique pair combinations where AB != BA
list(itertools.product('ABC',repeat=2)) # matches the permutation, the product of 'ABC' 'ABC'

# group by generator and reversed
list(itertools.groupby('LLLLAAGGG')) # returns a list of grouped objects 

for char, group in itertools.groupby('LLLLAAGGG'):
    print(char, '->', list(group))

# key 
animals = ['duck','goose','cat','python','mongoose','dog', 'frog','toad','snake','kangaroo','aligator','robin']

animals.sort(key=len)
animals 
for length, group in itertools.groupby(animals,len):
    print(length,'->',list(group))

for length, group in itertools.groupby(reversed(animals),len): # built-in reversed function
    print(length,'->',list(group))

# tee generator, generates multiple generators as output

list(itertools.tee('ABC'))

g1, g2 = itertools.tee('ABC')
# next(g1)
# next(g2)
# print(next(g1))
# print(next(g2)) 

list(zip(*itertools.tee('ABC')))
#itertools.tee creates 2 instances of generator by default 
#list(itertools.pairwise(next(g1) next(g2)))


# All and Any Generator Functions 
all([1,2,3]) # returns true if all values are truthy 
all([1,0,3])
all([]) # hate that this returns true 
any([]) # hate this equally as much

g = (n for n in [0,0.0,7,8]) # returns a generator obj
any(g) # can eval the generator object 
print(next(g)) # any iterated over g until a truthy was found then 8 was left 









