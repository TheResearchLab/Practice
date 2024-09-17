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