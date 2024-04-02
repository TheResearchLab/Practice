
import json
import pkg_resources
import pandas as pd
from datetime import datetime,timedelta
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import pickle
from typing import Tuple



def get_env_deps() -> str:
    installed_packages = [dist.project_name for dist in pkg_resources.working_set]
    installed_packages_dict = {pkg: pkg_resources.get_distribution(pkg).version for pkg in installed_packages}
    return json.dumps(installed_packages_dict, indent=None)

# create synthetic data
def get_synthetic_df(total_rows:int,year:int,month:int,day:int,std_dev:int) -> pd.DataFrame:

    dates = [datetime(year,month,day) + timedelta(days=i) for i in range(total_rows)]
    indep_var = np.arange(total_rows)
    dep_var = 2 * indep_var + np.random.normal(scale=std_dev, size=total_rows)
    category = np.random.choice(['A','B','C'], total_rows)

    data = {
        'date':dates,
        'indep_var':indep_var,
        'category':category,
        'dep_var':dep_var
    }

    df = pd.DataFrame.from_dict(data)
    df = pd.concat([df,pd.get_dummies(df['category']).astype(int)],axis=1)
    return df


# build audit table
def get_audit_df(dates:pd.Series) -> pd.DataFrame:
    audit = {
        'beg_dt': [min(dates),max(dates) + timedelta(days=1),max(dates) + timedelta(days=101)],
        'end_dt': [max(dates),max(dates) + timedelta(days=100),max(dates) + timedelta(days=200)],
        'model_pkl':[None,None,None],
        'model_metrics':[None,None,None],
        'model_deps': [None,None,None],
        'has_run': [False,False,None]
    }

    return pd.DataFrame.from_dict(audit)


# train model
def train_model(model_type:any,X_train:pd.DataFrame,y_train:pd.Series) -> str:
    model = model_type()
    model.fit(X_train,y_train)
    return model

# get model pickle
def get_model_pkl(model):
    return pickle.dumps(model).hex()


# get model performance measures
def get_model_metrics(model,X_test,y_test):
    return str({'test_score':model.score(X_test,y_test)})


def get_train_dates(audit_df:pd.DataFrame) -> Tuple[pd.Timestamp,pd.Timestamp]:
    beg_dt = audit_df[audit_df['has_run'] == False]['beg_dt'].min()
    end_dt = audit_df[audit_df['has_run'] == False]['end_dt'].min()
    return beg_dt,end_dt


def get_train_data(beg_dt:pd.Timestamp,end_dt:pd.Timestamp,df:pd.DataFrame) -> list:
    filtered_df = df[(df['date'] >= beg_dt) & (df['date']<=end_dt)]
    X = filtered_df.drop(['dep_var','date','category'],axis=1)
    y = filtered_df['dep_var']
    return train_test_split(X,y,test_size=.3,random_state=42)

# update audit (model,req,flag,performance_measure)
def update_audit_df() -> None:
    condition = audit_df['beg_dt'] == audit_df[audit_df['has_run'] == False]['beg_dt'].min()
    index = audit_df.loc[condition].index 
    audit_df.loc[index,'model_pkl'] = get_model_pkl(model)
    audit_df.loc[index,'model_metrics'] = get_model_metrics(model,X_test,y_test)
    audit_df.loc[index,'model_deps'] =  get_env_deps()
    audit_df.loc[index,'has_run'] = True

    audit_df.iloc[index]

    return audit_df


if __name__ == '__main__':

    # first 100 days
    df = get_synthetic_df(100,2023,1,1,5)
    audit_df = get_audit_df(df['date'])
    train_dates = get_train_dates(audit_df)
    X_train, X_test, y_train, y_test = get_train_data(train_dates[0],train_dates[1],df)
    model = train_model(LinearRegression,df.drop(['dep_var','date','category'],axis=1),df['dep_var'])
    update_audit_df()

    # next 100 days
    # df = pd.concat([df,get_synthetic_df(100,2023,4,11,13)])
    # train_dates = get_train_dates(audit_df)
    # X_train, X_test, y_train, y_test = get_train_data(train_dates[0],train_dates[1],df)
    # model = train_model(LinearRegression,df.drop(['dep_var','date','category'],axis=1),df['dep_var'])
    # update_audit_df()


    # check that model dependencies are the same
    # compare the model metrics to check for model drift

    # predict with model hex 
    #model_hex = data.loc[data.model_name == 'NBA Home Team Win Loss']['model_object_hex'][0]
    #pickle.loads(bytes.fromhex(model_hex)).score(scaled_data_test,y_test)


# %%


# Assumption 1 - Independent and Dependent Variables of the Model are Continuous



start_dt = datetime(2023,01,01)
today = start_dt + timedelta(100) # input is arbitrary can be any timeframe 


class DriverTable:
    
    def __init__(self,start_dt,increment,num_rows):
        pass

    @property
    def next_beg_dt():
        pass 

    @property
    def next_end_dt():
        pass

    def insert(self):
        """ Adds A New Row To Table For Future Run."""
        pass
    
    def update(self,model,eval_dict,env_deps):
        """ Update Driver Table Given Some Output From Model Training."""
        # model train output
        # model evaluation dict
        # env deps
        # has run flag
        pass

    def predict(self):
        pass 


class MyLinearModel:

    def __init__(self):
        """ Creates Simple Linear Regression Model For Example"""
        pass

    def train(self,X_train,y_train): #Update model pkl object attribute
        pass 

    def evaluate(self,X_test,y_test): #Update evaluation dict object attribute
        pass
    
    @staticmethod
    def get_env_dependencies() -> str: 
        """ Outputs Pip Freeze Results As Dict or String """


class DataGenerator:

    def __init__(self,start_dt,num_rows,num_cols):
        """ Generate Random Dataset For Use Case"""
        pass

    def get_training_data(self,beg_dt,end_dt) -> Tuple[pd.DataFrame,pd.Series,pd.DataFrame,pd.Series]:
        """ Return X and y Train & Test Data """
        pass 

    def generate_rows(self,num_rows):
        """ Simulates Data Generation Process """
    


# PART 1 - create model and predict using driver table
# create driver table & create synthetic data objects
# get train data from DataGenerator based on DriverTable beg and end_dts
# train MyLinearModel and input those object outputs into the DriverTable update method
# predict with the model from the DriverTable update

# PART 2 - create another model and compare models against each other
# insert 2 new rows into the DriverTable object and generate more rows for DataGenerator
# repeat steps from PART 1 to generate another model
# compare model evaluations and dependencies (maybe alter dependencies in-between training)
