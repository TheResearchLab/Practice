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

def demo() -> None:
    book = BookDict(
        isbn='0134757599',
        title='Refactoring, 2e',
        authors=['Martin Fowler', 'Kent Beck'],
        pagecount=478
    )
    authors = book['authors']
    if TYPE_CHECKING:
        reveal_type(authors)
    authors = 'Bob' # this should be a lsit
    book['weight'] = 4.2 # can't assign key that isn't part of the definition
    del book['title'] # cannot delete a key that is part of the definition

import json

# Code errors in mypy when using --disallow-any-expr flag
# def from_json(data: str) -> BookDict:
#     whatever = json.loads(data) # json.loads is Any so this line is the main issue
#     return whatever

# This code fixes error related to the above
def from_json(data:str) -> BookDict:
    whatever: BookDict = json.loads(data)
    return whatever


if __name__ == '__main__':
    demo()

# %%
