#%%


import pandas as pd

url = 'https://raw.githubusercontent.com/hadley/fueleconomy/master/data-raw/vehicles.csv'
df = pd.read_csv(url)

make = df.make

# Chapter 15.2 Frequency counts
#print(
#     make.value_counts(),
#     make.shape,
#    make.nunique()
# )

# Chapter 15.3  Benefits of Categories

cat_make = make.astype('category')
# print(
#     make.memory_usage(deep=True),
#     cat_make.memory_usage(deep=True) #lower memory usage
# )

# import timeit 
# code = """cat_make.str.upper()"""
# execution_time = timeit.timeit(stmt=lambda: eval(code),number=1)
# print(f'execution time: {execution_time}')

# code = """make.str.upper()""" #make is slower
# execution_time = timeit.timeit(stmt=lambda: eval(code),number=1)
# print(f'execution time: {execution_time}')


# Chapter 15.4 Conversion to Ordinal Categories
# Convert 'make' column to lowercase
make = make.str.lower()

# Create an ordered categorical type based on the sorted unique values
make_type = pd.CategoricalDtype(categories=sorted(make.unique()), ordered=True)

# Apply the categorical type to the 'make' column
ordered_make = make.astype(make_type)

# print(
#     ordered_make,
#     ordered_make.max(),
#     #cat_make.max() typeerror
#     ordered_make.sort_values()
# )

# Chapter 15.5 .cat Accessor
# cat_make.cat.rename_categories(
#     [c.upper() for c in cat_make.cat.categories] # rename categories with list of same length as current categories
# )

# cat_make.cat.rename_categories(
#     {c.lower() for c in cat_make.cat.categories} # or dictionary
# )

# ordered_make.cat.reorder_categories(
#      sorted(ordered_make.cat.categories, key=lambda x: x.lower()))


# Chapter 15.6 Category Gotchas 
#ordered_make.iloc[:100].value_counts() # returns 130 because uses all unique values

# (cat_make   
#     .iloc[:100]
#     .groupby(cat_make.iloc[:100])
#     .first()
# ) # also returns more than 100


# (make   
#     .iloc[:100]
#     .groupby(make.iloc[:100])
#     .first()
# ) # behavior is as expected with string?

# (cat_make
#     .iloc[:100]
#     .groupby(cat_make.iloc[:100],observed=True)
#     .first()
# ) # output looks like string output

# ordered_make.iloc[0] # returns a scalar
# ordered_make.iloc[[0]] # returns a categorical data type

# Chapter 15.7 Generalization 

def generalize_topn(ser,n=5,other='Other'):
    topn = ser.value_counts().index[:n]
    if isinstance(ser.dtype,pd.CategoricalDtype):
        ser = ser.cat.set_categories(
            topn.set_categories(list(topn)+[other]))
    return ser.where(ser.isin(topn),other)

cat_make.pipe(generalize_topn,n=20,other='NA')

def generalize_mapping(ser, mapping, default):
    seen = None
    res = ser.astype(str)
    for old, new in mapping.items():
        mask = ser.str.contains(old)
    
        if seen is None:
            seen = mask
        else:
            seen |= mask
        res = res.where(~mask, new)
    
    res = res.where(seen, default)
    return res.astype('category')
        
result = generalize_mapping(cat_make, {'Ford': 'US', 'Tesla': 'US', 'Chevrolet': 'US',
                                       'Dodge': 'US', 'Oldsmobile': 'US', 'Plymouth': 'US',
                                       'BMW': 'German'}, 'Other')

# Chapter 15.9 Exercises
str_column = pd.Series(['word' for i in range(0,1000)])
cat_column = str_column.astype('category')
    
    
print(
    str_column.memory_usage(deep=True),
    cat_column.memory_usage(deep=True) #saves about 46x the memory
)

num_column = pd.Series([i for i in range(0,1000)])
cat_column = pd.cut(num_column,bins=100).astype('category')


print(
    num_column.memory_usage(deep=True),
    cat_column.memory_usage(deep=True) #around 30% savings
)

print(
    cat_column.memory_usage(deep=True),
    generalize_topn(cat_column).memory_usage(deep=True) # 5x savings
)

# %%


