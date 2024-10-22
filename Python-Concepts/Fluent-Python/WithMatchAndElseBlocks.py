#%%

import sys 

class LookingGlass:

    def __enter__(self):
        self.original_write = sys.stdout.write 
        sys.stdout.write = self.reverse_write
        return 'JABBERWOCKY'
    
    def reverse_write(self,text):
        self.original_write(text[::-1])
    
    def __exit__(self,exc_type, exc_value, traceback):
        sys.stdout.write = self.original_write
        if exc_type is ZeroDivisionError:
            print('Please DO NOT divide by zero!')
            return 


with LookingGlass() as what:
    print('Alice, Kitty, and Snowdrop')
    print(what)

print(what) 
print('Back to normal.')

import contextlib 

@contextlib.contextmanager # A with statement, a generator, and function decorator
def looking_glass():
    original_write = sys.stdout.write 

    def reverse_write(text):
        original_write(text[::-1])
    
    sys.stdout.write = reverse_write 
    msg = ''
    try:
        yield 'JABBERWOCKY'
    except ZeroDivisionError:
        msg = 'Please DO NOT divide by zero!' 
    finally: # logic that will always execute, so can close sessions in code
        sys.stdout.write = original_write
        if msg:
            print(msg) 


with looking_glass() as what:
    print('Alice, Kitty, and Snowdrop')
    print(what)

print('back to normal')


# Else in For, Try, While loops 

my_list  = [{"flavor":"Banana"},{"flavor":"cherry"},{"flavor":"banana"}]

for item in my_list:
    if item["flavor"] == "banana":
        break 
else:
    raise ValueError('No banana flavor found!')

import random
char = ["cherry","blue","6","NaN","1.2","5i","car"][random.randint(1,6)]

# seperate the logic between the breaking logic and the remaining logic
try:
    value = int(char) # breaking logic
except ValueError as e:
    raise ValueError(e)
else:
    value+=1 # remaining logic

print(value)


