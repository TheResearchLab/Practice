#%% 
from typing import Optional,Any

# def show_count(count: int, singular: str, plural: Optional[str] = None) -> str:
#     if count == 1:
#         return f'1 {singular}'
#     count_str = str(count) if count else 'no'
#     return f'{count_str} {plural}s'

# def test_irregular() -> None:
#     got = show_count(2,'child','children')
#     assert got == '2 children'

# class Bird:
#     pass

# class Duck(Bird):
#     def quack(self):
#         print('Quack')

# def alert(birdie):
#     birdie.quack() # should be no problem here

# def alert_duck(birdie: Duck) -> None:
#     birdie.quack()

# def alert_bird(birdie: Bird) -> None:
#     birdie.quack() # this returns err in mypy because Bird type does not implement quack method

# any vs object
def double(x: Any) -> Any: # this works
    return x * 2

def double(x:object) -> object: # this breaks mypy because object doesn't support __mul__
    return x * 2

# GENERIC COLLECTIONS

def tokenize(text: str) -> list[str]:
    return text.upper().split()


# TYPE ALIASES
from typing import TypeAlias 

FromTo: TypeAlias = tuple[str,str] # creates a type alias for this custom type, more readable

from collections.abc import Sequence, Iterable, Hashable
from random import shuffle 
from typing import TypeVar 

T = TypeVar('T') # nasty, type var needs to be introduce in namespace before it can be used

def sample(population: Sequence[T], size: int) -> list[T]:
    if size < 1:
        raise ValueError('size must be >= 1')
    result = list(population)
    shuffle(result)
    return result[:size]

from decimal import Decimal
from fractions import Fraction
from collections import Counter

HashableT = TypeVar('HashableT',bound=Hashable)

def mode(data: Iterable[HashableT]) -> HashableT:
    pairs = Counter(data).most_common(1)
    if len(pairs) == 0:
        raise ValueError('no mode for empty data')
    return pairs[0][0]

AnyStr = TypeVar('AnyStr',bytes,str)







# %%
