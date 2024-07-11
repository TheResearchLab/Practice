#%%
from typing import TypedDict

class BookDict(TypedDict):
    isbn: str
    title: str
    authors: list[str]
    pagecount: int

# pp = BookDict(title='Programming Pearls',
#               authors='Jon Bentley',
#               isbn='12345678910',
#               pagecount=234)

# type(pp) # returns a plain dict

# #pp.title #return an error dict object has no attr title? because it is just a plain dict

# pp['title'] #this works
# BookDict.__annotations__


# TypeDict Enables Mypy to catch errors
from typing import TYPE_CHECKING

# def demo() -> None:
#     book = BookDict(
#         isbn='0134757599',
#         title='Refactoring, 2e',
#         authors=['Martin Fowler', 'Kent Beck'],
#         pagecount=478
#     )
#     authors = book['authors']
#     if TYPE_CHECKING:
#         reveal_type(authors)
#     authors = 'Bob' # this should be a lsit
#     book['weight'] = 4.2 # can't assign key that isn't part of the definition
#     del book['title'] # cannot delete a key that is part of the definition

import json

# Code errors in mypy when using --disallow-any-expr flag
# def from_json(data: str) -> BookDict:
#     whatever = json.loads(data) # json.loads is Any so this line is the main issue
#     return whatever

# This code fixes error related to the above
def from_json(data:str) -> BookDict:
    whatever: BookDict = json.loads(data)
    return whatever

AUTHOR_ELEMENT = '<AUTHOR>{}</AUTHOR>'

def to_xml(book: BookDict) -> str:
    elements: list[str] = []
    for key, value in book.items():
        if isinstance(value,list):
            elements.extend(
                AUTHOR_ELEMENT.format(n) for n in value)
        else:
            tag = key.upper()
            elements.append(f'<{tag}>{value}</{tag}>')
    xml = '\n\t'.join(elements)
    return f'<BOOK>\n\t{xml}\n</BOOK>'

# example 2 - static type checking is unable to prevent error with inherently dynamic code ie Any
def demo() -> None:
    NOT_BOOK_JSON = """
        {"title": "Andromed Strain",
         "flavor": "pistachio",
         "authors": true
        }            
        
    """

    not_book = from_json(NOT_BOOK_JSON)
    if TYPE_CHECKING:
        reveal_type(not_book)
        reveal_type(not_book['authors'])

    print(not_book)
    print(not_book['flavor'])

    xml = to_xml(not_book)
    print(xml)


# if __name__ == '__main__':
#     demo()


# Typing.cast to overcome the failures of nominal type checks

def cast(typ, val):
    """ Cast a value to a type.
    This returns the value unchanged. To the type checker this
    signals that the return value has the designated type, but
    at runtime we intentionally don't check anything (we want
    this to be as fast as possible)."""
    return val 


from typing import cast

def find_first_str(a: list[object]) -> str:
    index = next(i for i, x in enumerate(a) if isinstance(x, str))
    # We only get here if there's at least on string
    return cast(str, a[index])



# %%

import abc
import random

from collections.abc import Iterable
from typing import TypeVar, Generic 



class Tombola(abc.ABC):
    
    @abc.abstractmethod
    def load(self,iterable):
        """ Add items from an iterable"""

    @abc.abstractmethod
    def pick(self):
        """Remove item at random, return it.
        
        This method should raise 'LookupError' when the instance is empty
        """
    
    def loaded(self):
        """Return 'True' if there's at leadt 1 item, 'False' otherwise. """
        return bool(self.inspect())
    
    def inspect(self):
        """Return a sorted tuple with the items currently inside."""
        items = []
        while True:
            try:
                items.append(self.pick())
            except LookupError:
                break
            self.load(items)
            return tuple(items)
        

T = TypeVar('T')

class LottoBlower(Tombola, Generic[T]):

    def __init__(self,items: Iterable[T]) -> None:
        self._balls = list[T](items)

    def load(self, items: Iterable[T]) -> None:
        self._balls.extend(items)

    def pick(self) -> T:
        try:
            position = random.randrange(len(self._balls))
        except ValueError:
            raise LookupError('pick from empty LottoBlower')
        return self._balls.pop(position)

    def loaded(self) -> bool:
        return bool(self._balls)
    
    def inspect(self) -> tuple[T,...]:
        return tuple(self._balls)
    
        
