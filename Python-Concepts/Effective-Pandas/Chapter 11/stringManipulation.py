import pandas as pd

url = 'https://raw.githubusercontent.com/hadley/fueleconomy/master/data-raw/vehicles.csv'
df = pd.read_csv(url)
city_mpg = df['city08']
highway_mpg = df['highway08']

# Chapter 11.1 String and Objects
make = df.make
# print(
#     make, #object type
#     make.astype('string'),   
# )

# Chapter 11.2 Categorical Strings
# print(
#     make.astype('category')
# )

# Chapter 11.3 .str Accessor
# print(
#     make.str.lower(),
#     make.str.find('A')
# )

# Chapter 11.4 Searching
# print(
#     make.str.extract(r'([^a-z A-Z])'),
#     make
#         .str.extract(r'([^a-z A-Z])',expand=False)
#         .value_counts() #removing missing values and count non-missing values
# )

# Chapter 11.5 Splitting
age = pd.Series(['0-10','11-15','11-15','61-65','46-50'])
import random 

def between(row):
    return random.randint(*row.values)

# print(
#     age.str.split('-'),
#     age
#      .str.split('-',expand=True)
#      .iloc[:,0]
#      .astype(int),
#     age
#      .str.slice(-2)
#      .astype('int64'),
#     age
#      .str[-2:] #crazy syntax
#      .astype(int),
#     age
#      .str.split('-',expand=True)
#      .astype(int)
#      .mean(axis='columns'),
#     age
#      .str.split('-',expand=True)
#      .astype(int)
#      .apply(between,axis='columns')
# )

# Chapter 11.6 Optimizing .apply with Cython
    # Need to revisit

# Chapter 11.7 Replacing Text
print(
    make.str.replace('A','Å' ), # replace substring
    make.replace('A','Å',regex=True) #replace mapping of complete string
)

# Chapter 11.9 Exercises
alpha_num_series = pd.Series(['123abcλ','456qweλ'])
pattern = r'(\d+)'
non_ascii = r'([^\x00-\x7F]+)'

print(
    make.str.lower(),
    make.str.slice(1), #slice method is not index based
    make.str.slice(-3),
    alpha_num_series.str.extract(pattern), #regex sucks
    alpha_num_series.str.extract(non_ascii), #regex sucks
    pd.get_dummies(make.apply(list).explode()).groupby(level=0).sum()


)



