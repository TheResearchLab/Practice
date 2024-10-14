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




