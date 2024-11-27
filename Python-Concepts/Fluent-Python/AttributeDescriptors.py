""" LineItem Example Continued - A simple descriptor """
# class Quantity:

#     def __init__(self,storage_name):
#         self.storage_name = storage_name 

#     def __set__(self,instance,value):
#         if value > 0:
#             instance.__dict__[self.storage_name] = value 
#         else:
#             msg = f'{self.storage_name} must be > 0'
#             raise ValueError(msg)
    
#     def __get__(self,instance,owner):
#         return instance.__dict__[self.storage_name]
    
# class LineItem:

#     weight = Quantity('weight')
#     price = Quantity('price')

#     def __init__(self,description,price,weight):
#         self.description = description
#         self.price = price 
#         self.weight = weight 
    
#     def subtotal(self):
#         return self.weight * self.price 
    
# print(LineItem('Blue Car',0,12))

""" Automatic Naming of Storage Attributes """

# class Quantity:

#     def __set_name__(self,owner,name):
#         self.storage_name = name 
    
#     def __set__(self,instance,value):
#         if value > 0:
#             instance.__dict__[self.storage_name] = value 
#         else:
#             msg = f'{self.storage_name} must be > 0'
#             raise ValueError(msg)

# class LineItem:

#     weight = Quantity()
#     price = Quantity()

#     def __init__(self,description,price,weight):
#         self.description = description
#         self.price = price 
#         self.weight = weight 
    
#     def subtotal(self):
#         return self.weight * self.price 
    
# print(LineItem('Blue Car',0,12))

""" Leveraging Inheritance In Your Descriptor """
# import abc 

# class Validated(abc.ABC):
    
#     def __set_name__(self,owner,name):
#         self.storage_name = name 
    
#     def __set__(self,instance,value):
#         value = self.validate(self.storage_name, value)
#         instance.__dict__[self.storage_name] = value 
    
#     abc.abstractmethod
#     def validate(self, name, value):
#         """return validated value or raise ValueError"""

# class NonBlank(Validated): 
#     """ Get a string with at least 1 non white space"""
#     def validated(self, name, value):
#         value = value.strip()
#         if not value:
#             raise ValueError('f{name} cannot be blank')
      


# class Quantity(Validated):
#     """ a number greater than zero"""
#     def validated(self,name,value):
#         if value <= 0:
#             raise ValueError(f'{name} must be > 0')
#         return value 

# class LineItem:
#     description = NonBlank()
#     weight = Quantity()
#     price = Quantity()

#     def __init__(self,description,price,weight):
#         self.description = description 
#         self.price = price 
#         self.weight = weight 
    
#     def subtotal(self):
#         return self.weight * self.price 

""" Intro to Nonoverriding Descriptors """
def cls_name(obj_or_cls):
    cls = type(obj_or_cls)
    if cls is type:
        cls = obj_or_cls 
    return cls.__name__.split('.')[-1]

def display(obj):
    cls = type(obj)
    if cls is type:
        return f'<class {obj.__name__}>'
    elif cls in [type(None), int]:
        return repr(obj)
    else:
        return f'<{cls_name(obj)} object>'

def print_args(name, *args):
    psuedo_args = ', '.join(display(x) for x in args)
    print(f'-> {cls_name(args[0])}.__{name}__({psuedo_args})')

class Overriding:
    """ a.k.a data desriptor or enforced descriptor """
    def __get__(self,instance,owner):
        print_args('get', self, instance, owner)
    
    def __set__(self,instance,value):
        print_args('set',self,instance,value)
    
class OverridingNoGet:
    """ an overriding descriptor without __get__"""
    def __set__(self,instance,value):
        print_args('set',self,instance,value)

class NonOverriding:
    """ a.k.a non-data or shadowable descriptor """
    def __get__(self,instance,owner):
        print_args('get',self,instance,owner) # No set method means non-overriding

class Managed:
    over = Overriding()
    over_no_get = OverridingNoGet()
    non_over = NonOverriding()

    def spam(self):
        print(f'-> Manged.spam({display(self)})')
    
