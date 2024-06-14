#%%
from typing import TypedDict

class BookDict(TypedDict):
    isbn: str
    title: str
    authors: list[str]
    pagecount: int

pp = BookDict(title='Programming Pearls',
              authors='Jon Bentley',
              isbn='12345678910',
              pagecount=234)

type(pp) # returns a plain dict

#pp.title return an error dict object has no attr title? because it is just a plain dict

pp['title'] #this works
BookDict.__annotations__


# %%
