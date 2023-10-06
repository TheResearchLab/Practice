from __future__ import annotations

class Point:
    x:float
    y:float

    def __init__(self,x:float,y:float):
        self.x = x
        self.y = y

    def __repr__(self) -> str:
        return f"({self.x},{self.y})"

    def add(self,other:Point) -> Point:
        x:float = self.x + other.x
        y:float = self.y + other.y
        return Point(x,y)
    

def Main() -> None:
    a: Point = Point(1.5,1.2)
    b: Point = Point(2.3,2.8)
    c: Point = a.add(b)
    print(c) # repr output
    
if __name__ == "__main__":
    Main()