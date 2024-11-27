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


# from typing import Union, Any 
# from collections.abc import Iterable, Iterator 

# FieldNames = Union[str,Iterable[str]]


# def parse_identifiers(names: FieldNames) -> tuple[str,...]:
#     if isinstance(names,str):
#         names = names.replace(',', ' ').split()
#     if not all(s.isidentifier() for s in names):
#         raise ValueError('names must all be valid identifiers')
#     return tuple(names)


# def record_factory(cls_name: str, field_names: FieldNames) -> type[tuple]:

#     slots = parse_identifiers(field_names)

#     def __init__(self,*args,**kwargs) -> None:
#         attrs = dict(zip(self.__slots__,args))
#         attrs.update(kwargs)
#         for name, value in attrs.items():
#             setattr(self,name,value)
    
#     def __iter__(self) -> Iterator[Any]:
#         for name in self.__slots__:
#             yield getattr(self,name)
    
#     def __repr__(self):
#         values = ', '.join(f'{name}={value!r}' for name, value 
#                            in zip(self.__slots__,self))
#         cls_name = self.__class__.__name__
#         return f'{cls_name}({values})'
    
#     cls_attrs = dict(
#         __slots__=slots ,
#         __init__=__init__,
#         __iter__=__iter__,
#         __repr__=__repr__
#     )

#     return type(cls_name,(object,),cls_attrs)

# Dog = record_factory('Dog','name weight owner') # creating the dog class 
# rex = Dog('rex',30,'Aaron')
# print(rex)

# name, weight, _ = rex 
# print(name,weight)
# "{2}'s dog weighs {1}kg".format(*rex)
# rex.weight = 32 
# print(rex)
# print(Dog.__mro__)

""" Checked Example for __init_subclass__ """

from collections.abc import Callable 
from typing import Any, NoReturn, get_type_hints 

class Field:
    def __init__(self, name:str, constructor: Callable) -> None:
        if not callable(constructor)  or constructor is type(None):
            raise TypeError(f'{name!r} type hint must be callable')
        self.name = name 
        self.constructor = constructor
    
    def __set__(self,instance:Any, value: Any) -> None:
        if value is ...:
            value = self.constructor()
        else:
            try:
                value = self.constructor(value)
            except (TypeError,ValueError) as e:
                type_name = self.constructor.__name__
                msg = f'{value!r} is not compatible with {self.name}:{type_name}'
                raise TypeError(msg) from e 
        instance.__dict__[self.name] = value 

class Checked:
    @classmethod
    def _fields(cls) -> dict[str, type]:
        return get_type_hints(cls)
    
    def __init_subclass__(subclass) -> None:
        super().__init_subclass__()
        for name, constructor in subclass._fields().items():
            setattr(subclass, name, Field(name,constructor))
    
    def __init__(self,**kwargs: Any) -> None:
        for name in self._fields():
            value = kwargs.pop(name,...)
            setattr(self, name, value)
        if kwargs:
            self.__flag_unknown_attrs(*kwargs)

    def __flag_unknown_attrs(self, *names: str) -> NoReturn:
        plural = 's' if len(names) > 1 else ''
        extra = ', '.join(f'{name!r}' for name in names)
        cls_name = repr(self.__class__.__name__)
        raise AttributeError(f'{cls_name} object has no attribute{plural} {extra}')
    
    def _asdict(self)  -> dict[str,Any]:
        return {
            name: getattr(self,name)
            for name, attr in self.__class__.__dict__.items()
            if isinstance(attr,Field)
        }
    
    def __repr___(self) -> str:
        kwargs = ', '.join(f'{key}={value}' for key, value in self._asdict().items())
        return f'{self.__class__.__name__}({kwargs})'
    

class Movie(Checked):
    title: str 
    year: int 
    box_office: float 

movie = Movie(title='The God Father',year=1972,box_office=137)
print(movie.title)
print(movie)



