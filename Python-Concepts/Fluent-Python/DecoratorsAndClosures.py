#%%
# @decorate
# def target():
#     print('target')

# # is the same as 
# def target():
#     print('target')

# target = decorate(target)

def deco(func):
    def inner():
        func()
        print('running inner')
    return inner 

@deco
def target():
    print('running target')

target()
# %%
b = 4
def f1(a):
    print(a)
    print(b)
    b=8
f1(12) # errors because adding b as a defined var in func body makes it a local variable

#%%
# CLOSURES

class Averager:
    def __init__(self):
        self.series = []
    
    def __call__(self,new_value):
        self.series.append(new_value)
        total = sum(self.series)
        return total/len(self.series)
    
avg = Averager()
avg(10)
avg(20)
avg(10)


def make_averager(): # closure
    series = [] 

    def averager(new_value):
        series.append(new_value) # series is a free variable not bound in local scope
        total = sum(series)
        return total/len(series)
    
    return averager

avg = make_averager()
avg(10)
avg(20)
avg(10)

avg.__code__.co_varnames
avg.__code__.co_freevars # series


# wrong implementation

def make_averager():
    count = 0
    total = 0

    def averager(new_value):
        count+=1
        total+= new_value
        return total/count
    return averager

# errors because count = count + 1 is calling count before it was declared, series works because was never reassigned

# working implementation w/ nonlocal

def make_averager():
    count = 0
    total = 0

    def averager(new_value):
        nonlocal count,total
        count+=1
        total+= new_value
        return total/count
    return averager



# %%
