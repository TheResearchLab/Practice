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









