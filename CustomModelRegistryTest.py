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
import tensorflow as tf
import time as t
from CustomModelRegistry import DataGenerator, ModelRegistry, MyLinearModel, get_env_dependencies
from datetime import datetime
import numpy as np
import random
import matplotlib.pyplot as plt

# Set up Model Registry Table
driver_table = ModelRegistry(datetime(1,1,1),12,2)  # Start date limitation workaround
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
data = DataGenerator(start_dt, n_rows, n_cols, min_value, max_value, intercept, noise, slope)

# Get train/test data
X_train, X_test, y_train, y_test = data.get_train_test_data(datetime(2023,1,1), datetime(2023,4,11))

# Instantiate and train Linear Regression model
myModel = MyLinearModel()
myModel.train(X_train.values, y_train.values)
myModel.evaluate(X_test.values, y_test.values)

# Update driver table with the trained model
driver_table.update(myModel.model_pkl, myModel.model_eval, get_env_dependencies(), datetime(2023,4,13))
driver_table.table
driver_table.table.info()


tf.keras.backend.clear_session()

# Instantiate and compile TensorFlow neural network model
tf_model = tf.keras.Sequential([
    tf.keras.layers.Dense(128, activation='relu', input_shape=(4,)),
    tf.keras.layers.Dense(64, activation='relu'),
    tf.keras.layers.Dense(1)  # Assuming a regression task for y_train
])
tf_model.compile(optimizer='adam', loss='mean_squared_error')

# Convert X_train and y_train to NumPy arrays
X_train_np = np.array(X_train.values).astype(np.float32)
y_train_np = np.array(y_train.values).reshape(-1,1).astype(np.float32)


# Train TensorFlow model


print(tf.__version__)

# Convert X_train and y_train to NumPy arrays
X_train_np = np.array(X_train.values,dtype=np.float32)
y_train_np = np.array(y_train.values,dtype=np.float32).reshape(-1, 1)

# # Debugging Statements
# print("X_train_np shape:", X_train_np.shape)
# print("y_train_np shape:", y_train_np.shape)
# print("X_train_np dtype:", X_train_np.dtype)
# print("y_train_np dtype:", y_train_np.dtype)
# print("NaN in X_train_np:", np.isnan(X_train_np).any())
# print("NaN in y_train_np:", np.isnan(y_train_np).any())
# print("Infinite in X_train_np:", np.isinf(X_train_np).any())
# print("Infinite in y_train_np:", np.isinf(y_train_np).any())

print("X_train_np sample:\n", X_train_np[:5])  # Print first 5 rows of input data
print("y_train_np sample:\n", y_train_np[:5])  # Print first 5 rows of target data

# Train TensorFlow model
tf_model.fit(X_train_np, y_train_np, epochs=10, batch_size=32, verbose=0)




# # Function to measure prediction latency for any model
# def measure_prediction_latency(model, input_data, is_tensorflow=False):
#     start_time = t.perf_counter()
    
#     if is_tensorflow:
#         model.predict(input_data, verbose=0)
#     else:
#         model.predict(input_data)
    
#     end_time = t.perf_counter()
#     return end_time - start_time

# # Test prediction latency for both models
# num_iterations = 100
# prediction_times_lr = []
# prediction_times_tf = []

# for _ in range(num_iterations):
#     # Generate random input for testing
#     random_input = np.random.randint(1, 100, (1, X_train.shape[1])).astype(np.float32)  # Ensure float32 type
    
#     # Measure latency for Linear Regression model
#     lr_latency = measure_prediction_latency(myModel, random_input)
#     prediction_times_lr.append(lr_latency)
    
#     # Measure latency for TensorFlow model
#     tf_latency = measure_prediction_latency(tf_model, random_input, is_tensorflow=True)
#     prediction_times_tf.append(tf_latency)

# # Display average latency
# print(f"Average Linear Regression Prediction Latency: {np.mean(prediction_times_lr):.6f} seconds")
# print(f"Average TensorFlow Neural Network Prediction Latency: {np.mean(prediction_times_tf):.6f} seconds")

# # Plot the prediction latencies for both models
# plt.figure(figsize=(10, 6))
# plt.plot(prediction_times_lr, label='Linear Regression Model')
# plt.plot(prediction_times_tf, label='TensorFlow Neural Network Model')
# plt.xlabel('Iteration')
# plt.ylabel('Prediction Latency (seconds)')
# plt.title('Prediction Latency Comparison')
# plt.legend()
# plt.show()



#%%

import pickle
import zlib
import tensorflow as tf
from sklearn.linear_model import LinearRegression
from io import BytesIO

# Create a sample scikit-learn model (Linear Regression)
lr_model = LinearRegression()

# Create a sample TensorFlow model (Neural Network)
tf_model = tf.keras.Sequential([
    tf.keras.layers.Dense(128, activation='relu', input_shape=(100,)),
    tf.keras.layers.Dense(64, activation='relu'),
    tf.keras.layers.Dense(10, activation='softmax')
])

# Compile the TensorFlow model (just for formality)
tf_model.compile(optimizer='adam', loss='categorical_crossentropy')

# Serialize and compare both models

# Function to serialize and compress a model using pickle
def serialize_and_compress(model):
    # Pickle serialization (non-compressed)
    pickle_bytes = pickle.dumps(model)

    # Pickle serialization with compression (using zlib)
    compressed_pickle_bytes = zlib.compress(pickle_bytes)

    return pickle_bytes, compressed_pickle_bytes

# Function to calculate space saved
def calculate_space_saved(uncompressed_size, compressed_size):
    return ((uncompressed_size - compressed_size) / uncompressed_size) * 100

# Serialize and compare Linear Regression model
lr_pickle_bytes, lr_compressed_pickle_bytes = serialize_and_compress(lr_model)

# Serialize and compare TensorFlow model
tf_pickle_bytes, tf_compressed_pickle_bytes = serialize_and_compress(tf_model)

# Size comparison
lr_pickle_size = len(lr_pickle_bytes)
lr_compressed_pickle_size = len(lr_compressed_pickle_bytes)

tf_pickle_size = len(tf_pickle_bytes)
tf_compressed_pickle_size = len(tf_compressed_pickle_bytes)

# Calculate space saved for both models
lr_space_saved = calculate_space_saved(lr_pickle_size, lr_compressed_pickle_size)
tf_space_saved = calculate_space_saved(tf_pickle_size, tf_compressed_pickle_size)

# Output results
print(f"Linear Regression Model (non-compressed): {lr_pickle_size} bytes")
print(f"Linear Regression Model (compressed): {lr_compressed_pickle_size} bytes")
print(f"Linear Regression Model space saved: {lr_space_saved:.2f}%")

print(f"Neural Network Model (non-compressed): {tf_pickle_size} bytes")
print(f"Neural Network Model (compressed): {tf_compressed_pickle_size} bytes")
print(f"Neural Network Model space saved: {tf_space_saved:.2f}%")



#%%
import time as t
from datetime import datetime
import numpy as np
import tensorflow as tf
import matplotlib.pyplot as plt

# Generate dummy data
n_rows = 70  # Number of samples
n_cols = 4   # Number of features
np.random.seed(42)  # For reproducibility

# Create dummy input features (X) and target values (y)
X_dummy = np.random.rand(n_rows, n_cols).astype(np.float32) * 100  # Values between 0 and 100
y_dummy = (np.random.rand(n_rows) * 3000).astype(np.float32).reshape(-1, 1)  # Target values up to 3000

# Print shapes to verify
print(f"X_dummy shape: {X_dummy.shape}")  # Should be (70, 4)
print(f"y_dummy shape: {y_dummy.shape}")  # Should be (70, 1)

# Clear any previous Keras sessions
tf.keras.backend.clear_session()

# Instantiate and compile TensorFlow neural network model
tf_model = tf.keras.Sequential([
    tf.keras.layers.Dense(128, activation='relu', input_shape=(n_cols,)),  # Input shape matches X_dummy
    tf.keras.layers.Dense(64, activation='relu'),
    tf.keras.layers.Dense(1)  # Output for regression
])
tf_model.compile(optimizer='adam', loss='mean_squared_error')

# Train TensorFlow model
tf_model.fit(X_dummy, y_dummy, epochs=10, batch_size=8, verbose=0)  # Adjust batch size as needed

# Function to measure prediction latency for the model
def measure_prediction_latency(model, input_data):
    start_time = t.perf_counter()
    model.predict(input_data, verbose=0)
    end_time = t.perf_counter()
    return end_time - start_time

# Test prediction latency
num_iterations = 100
prediction_times_tf = []

for _ in range(num_iterations):
    # Generate random input for testing
    random_input = np.random.rand(1, n_cols).astype(np.float32) * 100  # Random input in the same range
    tf_latency = measure_prediction_latency(tf_model, random_input)
    prediction_times_tf.append(tf_latency)

# Display average latency
print(f"Average TensorFlow Neural Network Prediction Latency: {np.mean(prediction_times_tf):.6f} seconds")

# Plot the prediction latencies for the TensorFlow model
plt.figure(figsize=(10, 6))
plt.plot(prediction_times_tf, label='TensorFlow Neural Network Model')
plt.xlabel('Iteration')
plt.ylabel('Prediction Latency (seconds)')
plt.title('TensorFlow Prediction Latency')
plt.legend()
plt.show()
