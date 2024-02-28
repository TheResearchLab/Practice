#%%

# VARIABLES ARE NOT BOXES
# a = [1,2,3]
# b = a 
# a.append(4)
# b # b holds a reference of a

# # IDENTITY, EQUALITY, AND ALIASES
# charles = {'name':'Charles L. Dodgson','born':1832}
# lewis = charles
# lewis is charles # returns true
# print(id(lewis),id(charles)) # point to the same memory location

# lewis['balance'] = 900 
# charles # these are the same object

# alex = {'name':'Charles L. Dodgson','born':1832,'balance':900}
# alex == charles # true
# alex is not charles # true

# # COPIES ARE SHALLOW BY DEFAULT
# l1 = [3,[55,44],(7,8,9)]
# #l2 = list(l1) # this creates a copy
# l2 = l1[:] # short-hand to make copy
# l2 == l1 
# l2 is l1 # separate but equal

# l4 = [2,[44,55,66],(22,45,6)]
# l5 = list(l4)
# print(id(l4[2]),id(l5[2]))
# l5[2] += (3,7)
# print(l4)
# print(l5)
# print(id(l4[2]),id(l5[2])) # no longer the same object reference

#Deep and Shallow copies

# import copy

# class Bus:
#     def __init__(self,passengers=None):
#         if passengers is None:
#             self.passengers = []
#         else:
#             self.passengers = list(passengers) # protects us in the case of a list passed for input

#     def pick(self,name):
#         self.passengers.append(name)
    
#     def drop(self,name):
#         self.passengers.remove(name)


# bus1 = Bus(['Aaron','Charlie','Andrew','Dom'])
# bus2 = copy.copy(bus1)
# bus3 = copy.deepcopy(bus1)
# print(id(bus1.passengers),id(bus2.passengers),id(bus3.passengers))
# bus1.drop('Charlie')
# print(bus2.passengers) # the copy of the object was changed
# id(bus1.passengers),id(bus2.passengers),id(bus3.passengers) #strange behavoir the ids are the same
# #bus3.passengers 


# # cyclic reference
# a = [10,20]
# b = [a,30]
# a.append(b)
# a # infinite loop

# c = copy.deepcopy(a)
# c

# # FUNCTION PARAMETERS AS REFERNCES

# def f(a,b):
#     a+=b
#     return a 

# a = [1,2]
# b = [3,4]
# print(f(a,b)) # function changed the mutable value
# print(a,b)


# # MUTABLE TYPES AS PARAMETER DEFAULTS: BAD IDEA

# class HauntedBus:
#     def __init__(self,passengers=[]):
#         self.passengers = passengers
    
#     def pick(self,name):
#         self.passengers.append(name)
    
#     def drop(self,name):
#         self.passengers.remove(name)


# bus1 = HauntedBus()
# bus1.pick('Aaron')
# bus1.pick('Jack')
# bus2 = HauntedBus() # did not pass any passengers to this new bus
# print(bus1.passengers,bus2.passengers)
# bus1.passengers is bus2.passengers #They are the same

# dir(HauntedBus.__init__)
# HauntedBus.__init__.__defaults__ #hiding here
# HauntedBus.__init__.__defaults__[0] is bus2.passengers


# DEL AND GARBAGE COLLECTION
import weakref

s1 = {1,2,3}
s2 = s1
def bye():
    print('Cheesy goodbye')

ender = weakref.finalize(s1,bye)
ender.alive


del s1
ender.alive

print('reassigning to new var')
s2 = 'spam'
ender.alive
# %%
