""" Type is a metaclass - Class to create classes"""
# print(type(type(7)('7')))

# class Whatever:
#     pass 

# print(type(Whatever))
""" Building a class factory """
# class Dog:
#     def __init__(self,name,weight,owner):
#         self.name = name 
#         self.weight = weight 
#         self.owner = owner 

# rex = Dog('rex',29,'Aaron')
# print(rex)


from typing import Union, Any 
from collections.abc import Iterable, Iterator 

FieldNames = Union[str,Iterable[str]]


def parse_identifiers(names: FieldNames) -> tuple[str,...]:
    if isinstance(names,str):
        names = names.replace(',', ' ').split()
    if not all(s.isidentifier() for s in names):
        raise ValueError('names must all be valid identifiers')
    return tuple(names)


def record_factory(cls_name: str, field_names: FieldNames) -> type[tuple]:

    slots = parse_identifiers(field_names)

    def __init__(self,*args,**kwargs) -> None:
        attrs = dict(zip(self.__slots__,args))
        attrs.update(kwargs)
        for name, value in attrs.items():
            setattr(self,name,value)
    
    def __iter__(self) -> Iterator[Any]:
        for name in self.__slots__:
            yield getattr(self,name)
    
    def __repr__(self):
        values = ', '.join(f'{name}={value!r}' for name, value 
                           in zip(self.__slots__,self))
        cls_name = self.__class__.__name__
        return f'{cls_name}({values})'
    
    cls_attrs = dict(
        __slots__=slots ,
        __init__=__init__,
        __iter__=__iter__,
        __repr__=__repr__
    )

    return type(cls_name,(object,),cls_attrs)

Dog = record_factory('Dog','name weight owner') # creating the dog class 
rex = Dog('rex',30,'Aaron')
print(rex)

name, weight, _ = rex 
print(name,weight)
"{2}'s dog weighs {1}kg".format(*rex)
rex.weight = 32 
print(rex)
print(Dog.__mro__)



