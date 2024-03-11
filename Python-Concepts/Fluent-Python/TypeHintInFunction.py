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

# GENERIC MAPPINGS


# %%
