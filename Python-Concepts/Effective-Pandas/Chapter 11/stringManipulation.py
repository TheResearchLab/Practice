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
