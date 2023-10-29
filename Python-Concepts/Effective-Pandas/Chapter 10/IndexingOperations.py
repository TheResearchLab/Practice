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
print(
    city2.reset_index(),
    city2.reset_index(drop=True) 
)