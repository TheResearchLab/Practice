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
    
# Example Instances - Overriding Class
# obj = Managed()
# obj.over # call instance property
# Managed.over # call class property 
# obj.over = 7 
# obj.over
# obj.__dict__['over'] = 8
# print(vars(obj)) # vars gets an object's attributes?
# obj.over

# Example Instances - Overriding No Get Class 
# obj = Managed()
# obj.over_no_get # nothing retrieved because no get method 
# obj.over_no_get = 7 
# print(obj.over_no_get) 
# obj.__dict__['over_no_get'] = 9
# print(obj.over_no_get)
# obj.over_no_get = 7 # sets value to descriptor instance
# print(obj.over_no_get) # return 9 because when there is no get method defined, python hits the instance dict for values first 

# Example Instance - Non Overriding Descriptor 
# obj = Managed()
# obj.non_over 
# obj.non_over = 7 # no set will cause this value to store in instance 
# print(obj.non_over) 
# print(obj.__dict__['non_over'])
# Managed.non_over # class non_over attr still exists 
# del obj.non_over 
# obj.non_over # should return descriptor get

# Example Instance - Changes to Class Attributes (Monkey-Patching)
# obj = Managed()
# Managed.over = 1 
# Managed.non_over = 1 
# Managed.over_no_get = 1

# print(obj.over,obj.over_no_get,obj.non_over)

# Methods are nonoverriding descriptors
# obj = Managed()
# print(obj.spam) 
# print(Managed.spam)
# obj.spam = 5 
# print(obj.spam)


import collections 

class Text(collections.UserString):

    def __repr__(self):
        return 'Text({!r})'.format(self.data)
    
    def reverse(self):
        return self[::-1]

word = Text('forward')
print(word)
print(word.reverse())
print(Text.reverse(Text('backward')))
print(type(Text.reverse),type(word.reverse))
print(list(map(Text.reverse, ['repaid',(10,20,30), Text('stressed')])))
print(Text.reverse.__get__(word))
print(Text.reverse.__get__(None, Text))
print(word.reverse.__self__)
print(word.reverse.__func__ is Text.reverse) 