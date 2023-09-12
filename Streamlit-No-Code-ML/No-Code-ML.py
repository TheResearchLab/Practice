import streamlit as st
from sklearn.model_selection import train_test_split
import sklearn.metrics as metrics
import xgboost
import time as t
import numpy as np
import pandas as pd



def main() -> None:

    # Sklearn packages/module metadata

    model_selection_dict = { "Classification":{
            "LogisticRegression":"sklearn.linear_model",
            "XGBClassifier":"xgboost.sklearn",
            "SVC":"sklearn.svm",
            "KNeighborsClassifier":"sklearn.neighbors",
            "GaussianNB":"sklearn.naive_bayes",
            "BernoulliNB":"sklearn.naive_bayes",
            "LinearSVC":"sklearn.svm",
            "MultinomialNB":"sklearn.naive_bayes",
            "DecisionTreeClassifier":"sklearn.tree",
            "RandomForestClassifier":"sklearn.ensemble"
            },
            "Regression":{
            "LinearRegression":"sklearn.linear_model",
            "XGBRegressor":"xgboost.sklearn",
            "Lasso":"sklearn.linear_model",
            "ElasticNet":"sklearn.linear_model",
            "BayesianRidge":"sklearn.linear_model",
            "Ridge":"sklearn.linear_model",
            "KNeighborsRegressor":"sklearn.neighbors",
            "SVR":"sklearn.svm",
            "DecisionTreeRegressor":"sklearn.tree",
            "RandomForestRegressor":"sklearn.ensemble"
            }
    } 

    sklearn_data_dict = {"module":"sklearn.datasets",
                     "data":{
                         "Iris (Multi-Class Classification)":"load_iris",
                         "Diabetes (Regression)":"load_diabetes",
                         "Wine (Mult-Class Classification)":"load_wine",
                         "Breast Cancer (Binary Classification)":"load_breast_cancer"
                     } 
                   }

     
    data_preprocess_dict = {
        "StandardScaler": {"class":"StandardScaler","module_name":"sklearn.preprocessing"},
        "MinMaxScaler": {"class":"MinMaxScaler","module_name":"sklearn.preprocessing"},
    }


   #================== General Use Functions =====================#

    #import module class
    def import_class(module_name,class_name):
        try:
            module = __import__(module_name, globals(),locals(),[class_name])
        except ImportError:
            return None
        return vars(module)[class_name]

    
    def instantiate_obj(class_name,module_name):
        object_instance = import_class(str(module_name),str(class_name))
        return object_instance()
    
    #def input_data_check():


   #================== Model Instance Functions =====================#

    
    # instantiate new ml model 
    @st.cache_resource  
    def model_instance(algorithm_name,module_name): 
        model = import_class(str(module_name),str(algorithm_name))
        return model()
            

    def train_model(data_key,data_source,model):
        
        if hasattr(st.session_state,'data'):    
            X_train, X_test, y_train, y_test = st.session_state.data
            model.fit(X_train,y_train)
            return [model,X_test,y_test]
            
        else:
            X_train, X_test, y_train, y_test = train_test_split(str(data_key),data_source) #change function name
            model.fit(X_train,y_train)
            return [model,X_test,y_test]


   
   #================== Data Related Functions =====================#

    
    
    def train_test_split(tableName,data_source) -> list:
        
        
        def split_data(dataframe) -> None:
            X = dataframe.iloc[:,:-1]
            y = dataframe.iloc[:,-1:]
            st.session_state.data = train_test_split(X, y, test_size=0.2, random_state = 42) 
            st.session_state.dataCols = dataframe.columns
            return None


        if data_source == 'My Computer':
            df = pd.read_csv(data_key)
            split_data(df)
            st.session_state.feature_names = df.columns
            return st.session_state.data
        
        if data_source == 'Sklearn Dataset':
            #Instantiate sklearn data object
            dataset_name = sklearn_data_dict['data'][data_key]
            data_module = sklearn_data_dict['module']
            data_instance = instantiate_obj(dataset_name,data_module)
            df = pd.DataFrame(np.column_stack((data_instance['data'],data_instance['target'])),
                            columns=[*data_instance['feature_names'],'target'])
            split_data(df)
            return st.session_state.data
        
        
    
   #================== App frontend design =====================#
    
    
    st.sidebar.success('The Research Lab™')

    st.header(':green[No]-Code-ML')

    # Display the input values
    prediction_task = st.selectbox("Prediction Task",["Classification","Regression"])
    data_source = st.selectbox("Data Location",['My Computer','Sklearn Dataset'])
    if data_source == 'Sklearn Dataset':
        data_key = st.selectbox("Choose a Dataset",[i for i in sklearn_data_dict['data'].keys()])
    if data_source == 'My Computer':
        data_key = st.file_uploader('Upload Data as CSV')
    algorithm_name = st.selectbox("Algorithm Type", [i for i in model_selection_dict[prediction_task].keys()]) 
    

    # Create Buttons for Setting Model Parameters, Model Training, and Data Transformations 
    params_btn,transform_data_btn,train_btn,predict_btn = st.columns([0.07,0.06,0.04,0.04],gap="small")
    with params_btn:
        params_bool = st.selectbox('Set Model Params',['No','Yes'],index=0)
    with transform_data_btn:
        transform_data_bool = st.selectbox('Transform Data',['No','Yes'],index=0)
    with train_btn:
        train_model_bool = st.selectbox('Train',['No','Yes'],index=0)
    with predict_btn:
        predict_bool = st.selectbox('Predict',['No','Yes'],index=0)

    
        

    

   #================== Data Type Converter functions =====================#


    def bool_convert(value):
        bool_dict = {"True":True,"False":False}
        if value in ('True','False'):
            return bool_dict[value]
        else:
            return value
    
    def int_convert(value):
        try:
            value = int(value)
            return value
        except ValueError:
            try:
                if str(float(value)).endswith(".0"):
                    value = int(float(value))
                    return value   
            except ValueError:
                return value
            
    def float_convert(value):
        try:
            value = float(value)
            return value 
        except:
            return value

    def none_type_convert(value):
        if value == 'None':
            return None
        else:
            return value 
    

    #Dictionary to hold new parameters
    model_param_dict = {}
    placeholder = st.empty() 

    # Logic for getting model parameters and setting (Custom Form)
    if params_bool == 'Yes' and 'Yes' not in [transform_data_bool,train_model_bool,predict_bool]:

        with placeholder.form("hyperparam_form"):
            module_name = model_selection_dict[prediction_task][algorithm_name]
            model = model_instance(str(algorithm_name) ,str(model_selection_dict[prediction_task][algorithm_name]))
            model_param_dict = model.get_params() if not hasattr(st.session_state,"hyperparams") else st.session_state.hyperparams

            model = model.set_params(**model_param_dict)
            
        
            st.write(f" :green[{algorithm_name}] Hyperparameters")
            for key,value in model_param_dict.items():
                model_param_dict[key] = st.text_input(f"{key}",model_param_dict.get(key,value))
                model_param_dict[key] = float_convert(model_param_dict[key])
                model_param_dict[key] = int_convert(model_param_dict[key])
                model_param_dict[key] = bool_convert(model_param_dict[key])
                model_param_dict[key] = none_type_convert(model_param_dict[key])
            submitted = st.form_submit_button("Update Hyperparameters")

            if submitted:
                st.session_state.hyperparams = model_param_dict
                st.session_state.algorithm_name = algorithm_name
                placeholder.empty()
                
        


    # Logic for training a model
    if train_model_bool == 'Yes' and 'Yes' not in [params_bool,transform_data_bool,predict_bool]:

        model = model_instance(str(algorithm_name) ,str(model_selection_dict[prediction_task][algorithm_name]))
        trained_model,X_test,y_test = train_model(data_key,data_source,model)
        st.session_state.model = trained_model

        st.title("Model Accuracy")
        if prediction_task == 'Classification':
            report = metrics.classification_report(trained_model.predict(X_test),y_test,output_dict=True)
            st.table(pd.DataFrame(report).T)
            
        else:
            st.text(trained_model.score(X_test,y_test))


    
    if transform_data_bool == 'Yes' and 'Yes' not in [params_bool,train_model_bool,predict_bool]:
            if not hasattr(st.session_state,"data"):
                train_test_split(str(data_key),data_source)
                st.session_state.data_source = data_source 
                st.session_state.data_key = data_key
            if hasattr(st.session_state,"data_source") and st.session_state.data_source != data_source:
                train_test_split(str(data_key),data_source)
                st.session_state.data_source = data_source 
                st.session_state.data_key = data_key
            if hasattr(st.session_state,'data_key') and st.session_state.data_key != data_key:
                train_test_split(str(data_key),data_source)
                st.session_state.data_source = data_source 
                st.session_state.data_key = data_key
            

            
            X_train,X_test,y_train,y_test = st.session_state.data

            try:
                st.table(X_train.describe())
            except:
                st.table(pd.DataFrame(X_train,columns=st.session_state.feature_names[0:-1]).describe())


            dataTransformationForm = st.empty()
            with dataTransformationForm.form("transformData"):
                transformX = st.selectbox("Independent Variable Transformations",['None','StandardScaler','MinMaxScaler','Log-Transform'])
                transformY = st.selectbox("Dependent Variable Transformations",['None','Log-Transform'])
                submitted = st.form_submit_button("submit")
            
            if submitted:
                if transformX != 'None' and transformX != 'Log-Transform':
                    module_name = data_preprocess_dict[transformX]["module_name"]
                    objectClass = data_preprocess_dict[transformX]["class"]
                    data_preprocessor = instantiate_obj(objectClass,module_name)
                    X_train = data_preprocessor.fit_transform(X_train)
                    X_test = data_preprocessor.fit_transform(X_test)
                    st.success(f"X_train Shape:{X_train.shape}, X_test Shape:{X_test.shape}")

                
                if transformX == 'Log-Transform':
                    X_train = np.log(X_train)
                    X_test = np.log(X_test)
                    st.success(f"X_train Shape:{X_train.shape}, X_test Shape:{X_test.shape}")


                
                if transformY == 'Log-Transform':
                    y_train = np.log(y_train)
                    y_test = np.log(y_test)
                    st.success(f"Y_train Shape:{y_train.shape}, Y_test Shape:{y_test.shape}")
                st.session_state.data = [X_train,X_test,y_train,y_test]

                dataTransformationForm.empty()



    if predict_bool == 'Yes' and 'Yes' not in [params_bool,transform_data_bool,train_model_bool]:
        if not hasattr(st.session_state,'data'):
            train_test_split(str(data_key),data_source)
        elif hasattr(st.session_state,'hyperparams'):
            hyperparams = st.session_state.hyperparams
            model = model_instance(str(algorithm_name) ,str(model_selection_dict[prediction_task][algorithm_name]),hyperparams)
        elif not hasattr(st.session_state,'hyperparams') :
            model = model_instance(str(algorithm_name) ,str(model_selection_dict[prediction_task][algorithm_name]))
        if hasattr(st.session_state,'data_key') and st.session_state.data_key != data_key:
            train_test_split(str(data_key),data_source)
            st.session_state.data_source = data_source 
            st.session_state.data_key = data_key
        


        predictionForm = st.empty()
        preDict = {key:'' for key in st.session_state.dataCols[0:-1]}

        with predictionForm.form('predict_here'):
            for key,value in preDict.items():
                preDict[key] = st.text_input(f"{key}",model_param_dict.get(key,value))
            submitted = st.form_submit_button("submit")
            
        if submitted:
          trained_model,X_test,y_test = train_model(data_key,data_source,model)
          sampleData = np.array([int(feature) for key,feature in preDict.items()])
          sampleData = sampleData.reshape(1,len(sampleData))
          st.success(f'{trained_model.predict(sampleData)}')
          predictionForm.empty() 
          


    
    return None



main()