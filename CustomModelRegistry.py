#%%
# PART 1 - create model and predict using driver table
# create driver table & create synthetic data objects
# get train data from DataGenerator based on ModelRegistry beg and end_dts
# train MyLinearModel and input those object outputs into the ModelRegistry update method
# predict with the model from the ModelRegistry update

# PART 2 - create another model and compare models against each other
# insert 2 new rows into the ModelRegistry object and generate more rows for DataGenerator
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
from packaging.version import Version
import zlib
import polars as pl



def get_env_dependencies() -> str: 
    """ Outputs Pip Freeze Results As Dict or String """
    installed_packages = [dist.project_name for dist in pkg_resources.working_set]
    installed_packages_dict = {pkg: pkg_resources.get_distribution(pkg).version for pkg in installed_packages}
    return json.dumps(installed_packages_dict, indent=None)


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

    

class ModelRegistry:

    def __init__(self, start_dt, increment, n_rows, library='pandas'):
        self.start_dt = start_dt
        self.increment = increment
        self.n_rows = n_rows
        self.library = library

        if self.library == 'pandas':
            data = {
                'Beg_Date': [start_dt + timedelta(days=increment * i) for i in range(n_rows)],
                'End_Date': [start_dt + timedelta(days=(increment * (i + 1))) for i in range(n_rows)],
                'Model_Obj': [None for _ in range(n_rows)],
                'Model_Eval': [None for _ in range(n_rows)],
                'Env_Deps': [None for _ in range(n_rows)],
                'Run_Flag': [False for _ in range(n_rows)],
                'Run_Date': [None for _ in range(n_rows)]
            }
            self.table = pd.DataFrame.from_dict(data)
        elif self.library == 'polars':
            data = {
                'Beg_Date': [start_dt + timedelta(days=increment * i) for i in range(n_rows)],
                'End_Date': [start_dt + timedelta(days=(increment * (i + 1))) for i in range(n_rows)],
                'Model_Obj': [None for _ in range(n_rows)],
                'Model_Eval': [None for _ in range(n_rows)],
                'Env_Deps': [None for _ in range(n_rows)],
                'Run_Flag': [False for _ in range(n_rows)],
                'Run_Date': [None for _ in range(n_rows)]
            }
            self.table = pl.DataFrame(data)

    @property
    def next_beg_dt(self):
        return self.table.filter(pl.col('Run_Flag') == False)['Beg_Date'].min() 

    @property
    def next_end_dt(self):
        return self.table.filter(pl.col('Run_Flag') == False)['End_Date'].min()

    def _insert(self):
        """ Adds A New Row To Table For Future Run."""
        new_row = {
            'Beg_Date': self.table['Beg_Date'].max() + timedelta(days=self.increment),
            'End_Date': self.table['End_Date'].max() + timedelta(days=self.increment),
            'Model_Obj': None,
            'Model_Eval': None,
            'Env_Deps': None,
            'Run_Flag': False,
            'Run_Date': None
        }

        if self.library == 'pandas':
            self.table.loc[len(self.table)] = new_row
        elif self.library == 'polars':
            new_df = pl.DataFrame(new_row)
            self.table = self.table.vstack(new_df, in_place=True)

    def update(self, model, eval_dict, env_deps, run_date):
        """ Update Driver Table Given Some Output From Model Training."""
        # Use Polars filter to find the target row for updating
        if self.library == 'pandas':
            target_row = self.table['Beg_Date'].idxmin()
            if pd.isna(target_row):
                print("No available row to update.")
                return  # Exit if no available row

            self.table.loc[target_row, 'Model_Obj'] = model
            self.table.loc[target_row, 'Model_Eval'] = json.dumps(eval_dict)
            self.table.loc[target_row, 'Env_Deps'] = json.dumps(env_deps)
            self.table.loc[target_row, 'Run_Flag'] = True
            self.table.loc[target_row, 'Run_Date'] = run_date
        elif self.library == 'polars':
            # Find the target row index
            target_row_index = self.table['Beg_Date'].arg_min()

            # Construct the update expression
            self.table = self.table.with_columns([
                pl.when(pl.arange(0, self.table.height) == target_row_index)
                .then(model)
                .otherwise(pl.col('Model_Obj')).alias('Model_Obj'),
                pl.when(pl.arange(0, self.table.height) == target_row_index)
                .then(pl.lit(eval_dict))  # Use pl.lit() for literal value
                .otherwise(pl.col('Model_Eval')).alias('Model_Eval'),
                pl.when(pl.arange(0, self.table.height) == target_row_index)
                .then(pl.lit(env_deps))  # Use pl.lit() for literal value
                .otherwise(pl.col('Env_Deps')).alias('Env_Deps'),
                pl.when(pl.arange(0, self.table.height) == target_row_index)
                .then(True)
                .otherwise(pl.col('Run_Flag')).alias('Run_Flag'),
                pl.when(pl.arange(0, self.table.height) == target_row_index)
                .then(run_date)
                .otherwise(pl.col('Run_Date')).alias('Run_Date')
            ])

    import json  # Ensure you have imported this

    def predict(self, args,decompress=False):
        if self.library == 'polars':
            # Find the index of the max Beg_Date where Run_Flag is True
            target_row = self.table.filter(pl.col('Run_Flag') == True)
            
            if target_row.is_empty():
                raise ValueError("No model found to make a prediction.")

            target_row_index = target_row['Beg_Date'].arg_max()

            # Access the model object directly
            model_bytes = self.table[target_row_index, 'Model_Obj']

            # Now retrieve the Env_Deps
            env_deps_str = self.table[target_row_index, 'Env_Deps']
            try:
                model_scikit_version = json.loads(env_deps_str)['scikit-learn']
            except (json.JSONDecodeError, KeyError) as e:
                raise ValueError(f"Env_Deps JSON parsing error: {e}")

        else:  # For Pandas
            target_row = self.table[self.table['Run_Flag'] == True]
            
            if target_row.empty:
                raise ValueError("No model found to make a prediction.")

            target_row_index = target_row['Beg_Date'].idxmax()

            model_bytes = self.table.loc[target_row_index, 'Model_Obj']
            env_deps_str = self.table.loc[target_row_index, 'Env_Deps']

            try:
                model_scikit_version = json.loads(json.loads(env_deps_str))['scikit-learn']
            except (json.JSONDecodeError, KeyError) as e:
                raise ValueError(f"Env_Deps JSON parsing error: {e}")

        # Check if model environment is the same as the env
        if self._validate_env_req(model_scikit_version):
            if decompress:
                model = pickle.loads(zlib.decompress(model_bytes))
            else:
                model = pickle.loads(model_bytes)
            return model.predict(args)

        raise ValueError(f"Incompatible versions for model... update environment to scikit-learn version: {model_scikit_version}")




        

    def _validate_env_req(self, model_scikit_version):
        env_scikit_version = json.loads(get_env_dependencies())['scikit-learn']
        return env_scikit_version == model_scikit_version



class MyLinearModel:

    def __init__(self):
        """ Creates Simple Linear Regression Model For Example"""
        self.model = LinearRegression()
        self.model_pkl = pickle.dumps(self.model)
        self.model_eval = {}
        

    def train(self,X_train,y_train): #Update model pkl object attribute
        self.model = self.model.fit(X_train,y_train)
        self.model_pkl = pickle.dumps(self.model)

    def evaluate(self,X_test,y_test): #Update evaluation dict object attribute
        self.model_eval = {'test_score':self.model.score(X_test,y_test)}
    
        

def generate_requirements_file(requirement_json):
    packages = json.loads(requirement_json)
    
    with open('requirements.txt','w') as f:
        for package,version in packages.items():
            f.write(f"{package}=={version}\n")

# generate requirements.txt based on model env dependencies
# generate_requirements_file(get_env_dependencies())

    


# Main Function
def main():

    # Model Registry Table Example
    driver_table = ModelRegistry(datetime(2023,1,1),101,12)
    driver_table.table 

    # Generate Sample Data 
    n_rows = 100
    n_cols = 4
    min_value = 1
    max_value = 100
    noise = 0.5
    intercept = 5
    slope = 12
    start_dt = datetime(2023,1,1)

    # Instantiate Data Generator Object      
    data = DataGenerator(start_dt,n_rows,n_cols,min_value,max_value,intercept,noise,slope)
    # data.table.head(10) 

    # Model #1 Trainset
    X_train, X_test, y_train, y_test = data.get_train_test_data(datetime(2023,1,1),datetime(2023,4,11))


    # Instantiate Linear Model/supplemental information & Train
    myModel = MyLinearModel()
    myModel.train(X_train.values,y_train.values)
    myModel.evaluate(X_test.values,y_test.values)

    # update driver table with new model
    driver_table.update(myModel.model_pkl,myModel.model_eval,get_env_dependencies(),datetime(2023,4,13))
    driver_table.table

    # Make a prediction using the ModelRegistry object
    prediction = driver_table.predict(np.array([44, 44, 44, 44]).reshape(1, -1))[0]
    print(prediction)


    # Part 2 - Generate new rows
    n_rows = 100
    min_value = 20
    max_value = 1200
    noise = 100.6
    intercept = 12
    slope = 5

    # Generate more synthetic data
    data.add_rows(n_rows,min_value,max_value,noise,intercept,slope)

    # Train model based on new synthetic data
    X_train, X_test, y_train, y_test = data.get_train_test_data(datetime(2023,4,12),datetime(2023,7,22))

    myModel = MyLinearModel()
    myModel.train(X_train.values,y_train.values)
    myModel.evaluate(X_test.values,y_test.values)

    #update registry
    driver_table.update(myModel.model_pkl,myModel.model_eval,get_env_dependencies(),datetime(2023,7,23))
    driver_table.table

    # Make a prediction using the newest ModelRegistry object
    prediction = driver_table.predict(np.array([44, 44, 44, 44]).reshape(1, -1))[0]
    print(prediction)



if __name__ == "__main__":
    main()


