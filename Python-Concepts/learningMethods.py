
class Person:
    name:str
    age:int = 0

    def __init__(self,name:str):
        """Constructor to initiate attribute values"""
        self.name = name
        
    def say_hello(self) -> None:
        """ Greet the user"""
        print(f"hi my name is {self.name}")

def main() -> None: 
    """Entrypoint."""
    p1 = Person("Lil Jody")
    p1.say_hello()

if __name__ == "__main__":
    main()