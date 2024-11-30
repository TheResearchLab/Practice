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

# from collections.abc import Callable 
# from typing import Any, NoReturn, get_type_hints 

# class Field:
#     def __init__(self, name:str, constructor: Callable) -> None:
#         if not callable(constructor)  or constructor is type(None):
#             raise TypeError(f'{name!r} type hint must be callable')
#         self.name = name 
#         self.constructor = constructor
    
#     def __set__(self,instance:Any, value: Any) -> None:
#         if value is ...:
#             value = self.constructor()
#         else:
#             try:
#                 value = self.constructor(value)
#             except (TypeError,ValueError) as e:
#                 type_name = self.constructor.__name__
#                 msg = f'{value!r} is not compatible with {self.name}:{type_name}'
#                 raise TypeError(msg) from e 
#         instance.__dict__[self.name] = value 

# class Checked:
#     @classmethod
#     def _fields(cls) -> dict[str, type]:
#         return get_type_hints(cls)
    
#     def __init_subclass__(subclass) -> None:
#         super().__init_subclass__()
#         for name, constructor in subclass._fields().items():
#             setattr(subclass, name, Field(name,constructor))
    
#     def __init__(self,**kwargs: Any) -> None:
#         for name in self._fields():
#             value = kwargs.pop(name,...)
#             setattr(self, name, value)
#         if kwargs:
#             self.__flag_unknown_attrs(*kwargs)

#     def __flag_unknown_attrs(self, *names: str) -> NoReturn:
#         plural = 's' if len(names) > 1 else ''
#         extra = ', '.join(f'{name!r}' for name in names)
#         cls_name = repr(self.__class__.__name__)
#         raise AttributeError(f'{cls_name} object has no attribute{plural} {extra}')
    
#     def _asdict(self)  -> dict[str,Any]:
#         return {
#             name: getattr(self,name)
#             for name, attr in self.__class__.__dict__.items()
#             if isinstance(attr,Field)
#         }
    
#     def __repr___(self) -> str:
#         kwargs = ', '.join(f'{key}={value}' for key, value in self._asdict().items())
#         return f'{self.__class__.__name__}({kwargs})'
    

# class Movie(Checked):
#     title: str 
#     year: int 
#     box_office: float 

# movie = Movie(title='The God Father',year=1972,box_office=137)
# print(movie.title)
# print(movie)

""" Enhancing Classes with Class Decorator """
# from collections.abc import Callable 
# from typing import Any, NoReturn, get_type_hints 

# class Field:
#     def __init__(self, name:str, constructor: Callable) -> None:
#         if not callable(constructor)  or constructor is type(None):
#             raise TypeError(f'{name!r} type hint must be callable')
#         self.name = name 
#         self.constructor = constructor
    
#     def __set__(self,instance:Any, value: Any) -> None:
#         if value is ...:
#             value = self.constructor()
#         else:
#             try:
#                 value = self.constructor(value)
#             except (TypeError,ValueError) as e:
#                 type_name = self.constructor.__name__
#                 msg = f'{value!r} is not compatible with {self.name}:{type_name}'
#                 raise TypeError(msg) from e 
#         instance.__dict__[self.name] = value 

# def _fields(cls:type) -> dict[str,type]:
#     return get_type_hints(cls)

# def __init__(self: Any, **kwargs:Any) -> None:
#     for name in self._fields():
#         value = kwargs.pop(name,...)
#         setattr(self,name,value)
#     if kwargs:
#         self.__flag_unknown_attrs(*kwargs)

# def __setattr__(self: Any, name: str, value: Any) -> None:
#     if name in self._fields():
#         cls = self.__class__ 
#         descriptor = getattr(cls,name)
#         descriptor.__set__(self,value)
#     else:
#         self.__flag_unknown_attrs(name)

# def __flag_unknown_attrs(self: Any, *names:str) -> NoReturn:
#     plural = 's' if len(names) > 1 else ''
#     extra = ', '.join(f'{name!r}' for name in names)
#     cls_name = repr(self.__class__.__name__)
#     raise AttributeError(f'{cls_name} has no attribute{plural} {extra}')


# def _asdict(self:Any) -> dict[str,Any]:
#     return {
#         name: getattr(self,name)
#         for name, attr in self.__class__.__dict__.items()
#         if isinstance(attr,Field)
#     }

# def __repr__(self: Any) -> str:
#     kwargs = ', '.join(
#         f'{key}={value}' for key, value in self._asdict().items()
#     )
#     return f'{self.__class__.__name__}({kwargs})'

# def checked(cls:type) -> type:
#     for name, constructor in _fields(cls).items():
#         setattr(cls,name,Field(name,constructor))
    
#     cls._fields = classmethod(_fields) #type: ignore 

#     instance_methods = (
#         __init__,
#         __repr__,
#         __setattr__,
#         _asdict,
#         __flag_unknown_attrs,
#     )
#     for method in instance_methods:
#         setattr(cls, method.__name__,method)
    
#     return cls


# @checked 
# class Movie:
#     title: str 
#     year: int
#     box_office: float 

# movie = Movie(title='The Godfather',year=1972,box_office=137)
# print(movie.title)
# print(movie)

""" Evaluation Time Experiment """
# import builderlib

""" Eval Demo builderlib """
# from builderlib import Builder, deco, Descriptor 
# print('# evaldemo module start')

# @deco 
# class Klass(Builder):
#     print('# Klass body')

#     attr = Descriptor()

#     def __init__(self):
#         super().__init__()
#         print(f'# Klass.__init__({self!r})')

#     def __repr__(self):
#         return '<Klass instance>'
    
# def main():
#     obj = Klass()
#     obj.method_a()
#     obj.method_b()
#     obj.attr = 999 

# if __name__ == '__main__':
#     main()

# print('# evaldemo module end')

""" MetaBunch metaclass example """
# class MetaBunch(type):
#     def __new__(meta_cls, cls_name, bases, cls_dict):
#         defaults = {}

#         def __init__(self,**kwargs):
#             for name, default in defaults.items():
#                 setattr(self,name,kwargs.pop(name,default))
#             if kwargs:
#                 extra = ', '.join(kwargs)
#                 raise AttributeError(f'No slots left for: {extra!r}')
            
#         def __repr__(self):
#             rep = ', '.join(f'{name}={value}'
#                             for name, default in defaults.items()
#                             if (value:= getattr(self, name)) != default)
#             return f'{cls_name}({rep})'
            
#         new_dict = dict(__slots__=[],__init__=__init__,__repr__=__repr__)

#         for name, value in cls_dict.items():
#             if name.startswith('__') and name.endswith('__'):
#                 if name in new_dict:
#                     raise AttributeError(f"Can't set {name!r} in {cls_name!r}")
#                 new_dict[name] = value 
#             else:
#                 new_dict['__slots__'].append(name)
#                 defaults[name] = value 
#         return super().__new__(meta_cls, cls_name, bases,new_dict)

# class Bunch(metaclass=MetaBunch):
#     pass 

# class Point(Bunch):
#     x = 0.0
#     y = 0.0 
#     color = 'gray'

# print(Point(x=1.2,y=3,color='green'))
# p = Point()
# print(p)
# print((p.x,p.y,p.color))

# p = Point(x=21)
# p.y=42
# print(p)
# p.flavor = 'banana'

""" Metaclass Evaluation Time Experiment """
from builderlib import Builder, deco, Descriptor 
from metalib import MetaKlass 

@deco 
class Klass(Builder, metaclass=MetaKlass):
    print('# Klass body')

    attr = Descriptor()

    def __init__(self):
        super().__init__()
        print(f'# Klass.__init__({self!r})')
    
    def __repr__(self):
        return '<Klass instance>'
    
def main():
    obj = Klass()
    obj.method_a()
    obj.method_b()
    obj.method_c()
    obj.attr = 999 

if __name__ == '__main__':
    main()

print('# evaldemo_meta module end')

