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

class Quantity:

    def __set_name__(self,owner,name):
        self.storage_name = name 
    
    def __set__(self,instance,value):
        if value > 0:
            instance.__dict__[self.storage_name] = value 
        else:
            msg = f'{self.storage_name} must be > 0'
            raise ValueError(msg)

class LineItem:

    weight = Quantity()
    price = Quantity()

    def __init__(self,description,price,weight):
        self.description = description
        self.price = price 
        self.weight = weight 
    
    def subtotal(self):
        return self.weight * self.price 
    
print(LineItem('Blue Car',0,12))

""" Leveraging Inheritance In Your Descriptor """
