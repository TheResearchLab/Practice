
#%%

# PART 1 - create model and predict using driver table
# create driver table & create synthetic data objects
# get train data from DataGenerator based on DriverTable beg and end_dts
# train MyLinearModel and input those object outputs into the DriverTable update method
# predict with the model from the DriverTable update

# PART 2 - create another model and compare models against each other
# insert 2 new rows into the DriverTable object and generate more rows for DataGenerator
# repeat steps from PART 1 to generate another model
# compare model evaluations and dependencies (maybe alter dependencies in-between training)


import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from typing import Tuple
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import pickle
import json
import pkg_resources



class DataGenerator:

    def __init__(self,start_dt,n_rows,n_cols,min_value,max_value,intercept,noise,slope):
        """ Generate Random Dataset For Use Case"""
        self.n_rows = n_rows
        self.n_cols = n_cols
        self.min_value = min_value
        self.max_value = max_value
        self.noise = noise
        self.intercept = intercept
        self.slope = slope
        self.start_dt = start_dt
        
        dates = [start_dt + timedelta(days=i) for i in range(n_rows)]
        X = np.random.uniform(min_value,max_value,size=(n_rows,n_cols))
        y = np.sum(np.dot(X,slope), axis=1) + intercept + np.random.normal(0,noise,size=n_rows)

        data = {'Date':dates}
        for i in range(n_cols):
            data[f'Feature_{i+1}'] = X[:,i] 
        data['Target'] = y 

        self.table = pd.DataFrame.from_dict(data)
        

    def get_train_test_data(self,beg_dt,end_dt) -> Tuple[pd.DataFrame,pd.Series,pd.DataFrame,pd.Series]:
        """ Return X and y Train & Test Data """
        X = self.table[(self.table['Date'] >= beg_dt) & (self.table['Date'] < end_dt)].drop(['Date','Target'],axis=1)
        y = self.table[(self.table['Date'] >= beg_dt) & (self.table['Date'] < end_dt)]['Target']
        return train_test_split(X,y,test_size=.3)

    
    def add_rows(self,n_rows,min_value,max_value,intercept,noise,slope):
        """ Simulates Data Generation Process """
        new_data_generator = DataGenerator.__new__(DataGenerator)
        start_dt = self.start_dt + timedelta(days = self.n_rows)
        
        new_data_generator.__init__(
            start_dt= start_dt,
            n_rows=n_rows,
            n_cols=self.n_cols,
            min_value=min_value,
            max_value = max_value,
            intercept = intercept,
            noise = noise,
            slope = slope)
        
        self.table = pd.concat([self.table,new_data_generator.table],ignore_index=True)

    

class DriverTable:
    
    def __init__(self,start_dt,increment,n_rows):
        self.start_dt = start_dt
        self.increment = increment
        self.n_rows = n_rows

        data = {
            'Beg_Date': [start_dt + timedelta(days=increment*i) for i in range(n_rows)],
            'End_Date': [start_dt + timedelta(days=(increment*(i+1))) for i in range(n_rows)],
            'Model_Obj': [None for i in range(n_rows)],
            'Model_Eval': [None for i in range(n_rows)],
            'Env_Deps': [None for i in range(n_rows)],
            'Run_Flag': [False for i in range(n_rows)],
            'Run_Date': [None for i in range(n_rows)]
        }
        
        self.table = pd.DataFrame.from_dict(data)

    @property
    def next_beg_dt(self):
        return self.table[self.table['Run_Flag'] == False]['Beg_Date'].min() 

    @property
    def next_end_dt(self):
        return self.table[self.table['Run_Flag'] == False]['End_Date'].min()

    def insert(self):
        """ Adds A New Row To Table For Future Run."""
        new_row = {
            'Beg_Date':self.table['Beg_Date'].max() + timedelta(days=self.increment),
            'End_Date':self.table['End_Date'].max() + timedelta(days=self.increment),
            'Model_Obj':None,
            'Model_Eval':None,
            'Env_Deps':None,
            'Run_Flag':False,
            'Run_Date':None
        }
        self.table.loc[len(self.table)] = new_row
    
    def update(self,model,eval_dict,env_deps,run_date):
        """ Update Driver Table Given Some Output From Model Training."""
        target_row = self.table[self.table['Run_Flag'] == False]['Beg_Date'].idxmin()

        # update row values
        self.table.loc[target_row,'Model_Obj'] = model
        self.table.loc[target_row,'Model_Eval'] = str(eval_dict)
        self.table.loc[target_row,'Env_Deps'] = str(env_deps)
        self.table.loc[target_row,'Run_Flag'] = True
        self.table.loc[target_row,'Run_Date'] = run_date
        

    def predict(self,args):
        target_row = self.table[self.table['Run_Flag'] == True]['Beg_Date'].idxmax() 
        model_hex = self.table.loc[target_row,'Model_Obj']
        model = pickle.loads(bytes.fromhex(model_hex))
        return model.predict(args)


class MyLinearModel:

    def __init__(self):
        """ Creates Simple Linear Regression Model For Example"""
        self.model = LinearRegression()
        self.model_pkl = pickle.dumps(self.model).hex()
        self.model_eval = {}
        

    def train(self,X_train,y_train): #Update model pkl object attribute
        self.model = self.model.fit(X_train,y_train)
        self.model_pkl = pickle.dumps(self.model).hex()

    def evaluate(self,X_test,y_test): #Update evaluation dict object attribute
        self.model_eval = {'test_score':self.model.score(X_test,y_test)}
    
    @staticmethod
    def get_env_dependencies() -> str: 
        """ Outputs Pip Freeze Results As Dict or String """
        installed_packages = [dist.project_name for dist in pkg_resources.working_set]
        installed_packages_dict = {pkg: pkg_resources.get_distribution(pkg).version for pkg in installed_packages}
        return json.dumps(installed_packages_dict, indent=None)
        





# Driver Table Example User

driver_table = DriverTable(datetime(2023,1,1),101,12)
driver_table.next_beg_dt
driver_table.next_end_dt
driver_table.insert()
driver_table.table






# Main Function

# Part 1 - Initial data generation
n_rows = 100
n_cols = 4
min_value = 1
max_value = 100
noise = 0.5
intercept = 5
slope = 12
start_dt = datetime(2023,1,1)

data = DataGenerator(start_dt,n_rows,n_cols,min_value,max_value,intercept,noise,slope)
data.table # this returns the dataframe

X_train, X_test, y_train, y_test = data.get_train_test_data(datetime(2023,1,1),datetime(2023,4,11))



myModel = MyLinearModel()
myModel.train(X_train.values,y_train.values)
myModel.model_eval
myModel.evaluate(X_test.values,y_test.values)
myModel.model_eval
myModel.get_env_dependencies()
myModel.model_pkl

driver_table.update(myModel.model_pkl,myModel.model_eval,myModel.get_env_dependencies(),datetime(2023,4,13))
driver_table.table

# Make a prediction using the DriverTable object
prediction = driver_table.predict(np.array([44, 44, 44, 44]).reshape(1, -1))[0]
prediction




# Part 2 - Generate new rows
# n_rows = 100
# min_value = 20
# max_value = 120
# noise = 1.6
# intercept = 12
# slope = 5

# data.add_rows(n_rows,min_value,max_value,noise,intercept,slope)
#data.table[(data.table['Date'] >= datetime(2023,1,1)) & (data.table['Date'] < datetime(2023,4,11))].drop(['Date','Target'],axis=1)





# %%
