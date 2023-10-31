# Indexing Operations 
import pandas as pd

url = 'https://raw.githubusercontent.com/hadley/fueleconomy/master/data-raw/vehicles.csv'
df = pd.read_csv(url)
city_mpg = df['city08']
highway_mpg = df['highway08']


# Chapter 10.1 Data Prep
city2 = city_mpg.rename(df['make'].to_dict())

#print(city2.index)

#Passing scalar values
city2 = city_mpg.rename(df['make'])
#print(city2)

# Chapter 10.2 Resetting the Index
# print(
#     city2.reset_index(),
#     city2.reset_index(drop=True) 
# )

# Chapter 10.3 The .loc Attribute 

mask = city2 > 50
# print(
#     city2.loc['Subaru'], # returns all values indexed with subaru
#     city2.loc[['Subaru']], # ensures a series is returned
#     city2.loc[['Subaru','Lamborghini']], # ensures a series is returned
#     #city2.iloc['Subaru':'Lamborghini'], # this is wrong, need to sort first
#     city2.sort_index().loc["Subaru":"Lamborghini"],
#     city2.sort_index().loc['F':'J'], #grabs anything that starts with F thru precisely 'J'
#     city2.loc[pd.Index(['Dodge'])], # Pass pandas index as series index
#     city2.loc[pd.Index(['Dodge','Dodge'])], # duplicates cause twice as many entries
#     city2.loc[mask] # indexing based on boolean
# )

# using function to index
cost = pd.Series([1.00,2.25,3.99,.99,2.79])
inflation = 1.10
# print(
#     cost.mul(inflation)
#         .loc[lambda s_: s_ > 3] #multiply the cost by inflation and see where value > 3
# )

# Chapter 10.4 the .iloc Attribute
# print(
#     city2.iloc[0], #returns a number
#     city2.iloc[[0]], #returns a series
#     city2.iloc[[1,-2]],
#     city2.iloc[0:5], # first 5 items not including 0->4
#     city2.iloc[-8:] # return the last 8 values
# )

mask = city2 > 50 
#city2.iloc[mask] doesn't work
# print(
#     city2.iloc[mask.to_numpy()], #can search like loc but need to be a numpy array or python list
#     city2.iloc[list(mask)]
# )

# Chapter 10.5 Heads and Tails
print(
    city2.head(3),
    city2.tail(3)
)

