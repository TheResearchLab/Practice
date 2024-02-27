#%%

# VARIABLES ARE NOT BOXES
a = [1,2,3]
b = a 
a.append(4)
b # b holds a reference of a

# IDENTITY, EQUALITY, AND ALIASES
charles = {'name':'Charles L. Dodgson','born':1832}
lewis = charles
lewis is charles # returns true
print(id(lewis),id(charles)) # point to the same memory location

lewis['balance'] = 900 
charles # these are the same object

alex = {'name':'Charles L. Dodgson','born':1832,'balance':900}
alex == charles # true
alex is not charles # true

# COPIES ARE SHALLOW BY DEFAULT
l1 = [3,[55,44],(7,8,9)]
#l2 = list(l1) # this creates a copy
l2 = l1[:] # short-hand to make copy
l2 == l1 
l2 is l1 # separate but equal

l4 = [2,[44,55,66],(22,45,6)]
l5 = list(l4)
print(id(l4[2]),id(l5[2]))
l5[2] += (3,7)
print(l4)
print(l5)
print(id(l4[2]),id(l5[2])) # no longer the same object reference



# %%
