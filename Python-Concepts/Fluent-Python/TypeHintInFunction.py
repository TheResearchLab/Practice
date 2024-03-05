#%% 
from typing import Optional

def show_count(count: int, singular: str, plural: Optional[str] = None) -> str:
    if count == 1:
        return f'1 {singular}'
    count_str = str(count) if count else 'no'
    return f'{count_str} {plural}s'

def test_irregular() -> None:
    got = show_count(2,'child','children')
    assert got == '2 children'


