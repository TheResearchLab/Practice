#%%
""" 
################################## Main checks #########################################
Test 1 A) Check inference times as you add more entries into the model registry table. This will only check how model performs as it is only searching for the most recent model for a single model.
Test 1 B) Check max # of inferences that can be made per minute as model registry table grows
Test 2 A) Check inference times as you add more entries of different model types requiring a selection of the most recent model of a given type... next will check for  most recent model, model type, and project id
Test 2 B) Check max # of inferences that can be  made per minutes as model registry table grows.
Test 3 A) Check inference times as you add more entries of different model types and project ids to the model registry table
Test 3 A) Check max 3 of inferences that can be made per minute as the model registry table grows
################################## Other checks #########################################
- concurrency testing
- pickling and loading overhead?
- pandas vs polars vs 

"""

import time as t 
from CustomModelRegistry import DataGenerator, ModelRegistry, MyLinearModel, get_env_dependencies 
from datetime import datetime, timedelta
import numpy as np 
import random 
import matplotlib.pyplot as plt





# # Model Registry Table Example
driver_table = ModelRegistry(datetime(1,1,1),12,2) # date starts at 1/1/1 because limitations hit with pandas dates. Max precision max_allowed_date = pd.Timestamp('2262-01-01')
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




num_iterations = 50000

prediction_times = []

for i in range(1,num_iterations):
    try:
        year = random.randint(2000,2025)
        month = random.randint(1,12)
        day = random.randint(1,28)

        run_date = datetime(year,month,day)
        # add record
        driver_table.update(myModel.model_pkl,myModel.model_eval,get_env_dependencies(),run_date)
        driver_table._insert()

        start_time = t.perf_counter()
        # time to predict
        driver_table.predict(np.array([44, 44, 44, 44]).reshape(1, -1))[0]
        prediction_latency = t.perf_counter()
        prediction_times.append(prediction_latency - start_time)
    except Exception as e:
        print(i,e)
        break




x = list(range(len(prediction_times)))
plt.plot(x,prediction_times,marker='o',linestyle='-')
print(prediction_times[0],prediction_times[-1])
#driver_table.table

#%%

import joblib
import pickle
import zlib
from sklearn.linear_model import LinearRegression
from io import BytesIO

# Create a sample model
model = LinearRegression()


joblib_buffer = BytesIO()
pickle_buffer = BytesIO()

# Joblib serialization (in-memory)
with joblib_buffer:
    joblib.dump(model, joblib_buffer,compress=3)
    joblib_data = joblib_buffer.getvalue()

# Pickle serialization (in-memory)
with pickle_buffer:
    pickle_byte = pickle.dumps(model)
    compressed_pickle_bytes = zlib.compress(pickle_byte)

# Size comparison
joblib_size = len(joblib_data)
pickle_size = len(compressed_pickle_bytes)

print(f"Joblib Size: {joblib_size} bytes")
print(f"Pickle Bytes Size: {pickle_size} bytes")