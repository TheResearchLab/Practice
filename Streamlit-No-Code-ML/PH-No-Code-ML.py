from snowflake.snowpark import Session
import streamlit as st
import cloudpickle as cp
from sklearn.model_selection import train_test_split
import xgboost
import time as t
import numpy as np
import pandas as pd

email = ''
schema = ''


@st.experimental_singleton
def createConnection():
    connection_parameters = {
        "account": "spectrumhealth-idea",
        "user": f"{email}",
        "authenticator": "externalbrowser",
        "role": "SFK_PH_IDEA_DVLPR_U",
        "warehouse": "PH_IDEA_M_WH",
        "database": "PH_IDEA_PLAY",
        "schema": f"{schema}"}

    return Session.builder.configs(connection_parameters).create()


new_session = createConnection()




def main() -> None:

    # Sklearn packages/module metadata

    modelDict = { "Classification":{
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

    sklearnData = {"module":"sklearn.datasets",
                     "data":{
                         "Iris (Multi-Class Classification)":"load_iris",
                         "Diabetes (Regression)":"load_diabetes",
                         "Wine (Mult-Class Classification)":"load_wine",
                         "Breast Cancer (Binary Classification)":"load_breast"
                     } 
                   }


   #================== Model Instance Functions =====================#

    
    # instantiate new ml model 
    @st.experimental_singleton  
    def modelInstance(_session,modelName,moduleName,hyperparams={}):
        modelOutput = _session.call("InstantiatePythonObject",str(modelName),'Model',str(moduleName))
        if hyperparams == {}:
            return cp.loads(bytes.fromhex(modelOutput))
        else:
            return cp.loads(bytes.fromhex(modelOutput)).set_params(**hyperparams)
            

    def modelTrain():
        if hasattr(st.session_state,'data'):    
            X_train, X_test, y_train, y_test = st.session_state.data #Only Supports Snowflake data for now
            model.fit(X_train,y_train)
            return [model,X_test,y_test]
            
        else:
            X_train, X_test, y_train, y_test = getTrainAndTestData(new_session,str(modelData),dataStore) #Only Supports Snowflake data for now
            model.fit(X_train,y_train)
            return [model,X_test,y_test]
   #================== Data Related Functions =====================#

    # Data processors help perform statistics based transformations
    def pythonObjectInstance(session,className,moduleName):
        pythonObjectInst = session.call("InstantiatePythonObject",str(className),'Other',str(moduleName))
        return cp.loads(bytes.fromhex(pythonObjectInst))
    
    def trainTestSplit(dataframe) -> None:
        X = dataframe.iloc[:,:-1]
        y = dataframe.iloc[:,-1:]
        st.session_state.data = train_test_split(X, y, test_size=0.2, random_state = 42) #parameterize in future
        st.session_state.dataCols = dataframe.columns
        return None
    
    
    def getTrainAndTestData(session,tableName,dataStore) -> list:
        if dataStore == 'My Computer':
            return None
        
        if dataStore == 'Sklearn Dataset':
            dataInst = pythonObjectInstance(new_session,sklearnData['data'][modelData],sklearnData['module'])
            df = pd.DataFrame(np.column_stack((dataInst['data'],dataInst['target'])),
                            columns=[*dataInst['feature_names'],'target'])
            trainTestSplit(df)
            return st.session_state.data
        
        if dataStore == 'Snowflake':
            df = session.table(tableName).to_pandas()
            trainTestSplit(df)
            return st.session_state.data
        

    
    
    def inputDataCheck(dataStore,modelData) -> bool:
        if dataStore == 'My Computer':
            st.error("Self Uploads Are Currently Not Supported", icon='☹')
            return False
        if dataStore == 'Snowflake' and modelData == '':
            st.error("Please Enter A Table To Build Your Model", icon='☹')
            return False
        else:
            return True
        
    
    
   #================== App frontend design =====================#
    
    
    st.sidebar.success('© 2023 The Top Secret Data Science Club™')

    st.header(':green[PH]-No-Code-ML')

    # Display the input values
    predictionTask = st.selectbox("Prediction Task",["Classification","Regression"])
    dataStore = st.selectbox("Data Location",['Snowflake','My Computer','Sklearn Dataset'])
    if dataStore == 'Snowflake':
        modelData =  st.text_input("Table/View Name")
    if dataStore == 'Sklearn Dataset':
        modelData = st.selectbox("Choose a Dataset",[i for i in sklearnData['data'].keys()])
    if dataStore == 'My Computer':
       modelData = st.file_uploader('Upload Data')
    algoType = st.selectbox("Algorithm Type", [i for i in modelDict[predictionTask].keys()]) 
    

    # Create Buttons for Setting Model Parameters, Model Training, and Data Transformations 
    paramsBtn,dataTransformBtn,trainBtn,predictBtn,saveBtn = st.columns([0.07,0.06,0.04,0.04,0.04],gap="small")
    with paramsBtn:
        getParams = st.selectbox('Manually Set Params',['No','Yes'],index=0)
    with dataTransformBtn:
        dataChanges = st.selectbox('Transform Data',['No','Yes'],index=0)
    with trainBtn:
        trainModel = st.selectbox('Train',['No','Yes'],index=0)
    with saveBtn:
        saveModel = st.selectbox('Save',['No','Yes'],index=0)
    with predictBtn:
        predictModel = st.selectbox('Predict',['No','Yes'],index=0)

    
    
    def updateHyperparameters(modelParamDict,algoType) -> None:
        st.session_state.hyperparams.update(modelParamDict)
        st.session_state.algoType = algoType
        st.text(st.session_state.hyperparams)
        st.success('Updated Hyperparameters')
        
    
    

    

   #================== App Event Listeners =====================#

    #Dictionary to hold new parameters
    modelParamDict = {}
    placeholder = st.empty() 

    def boolConverter(value):
        boolDict = {"True":True,"False":False}
        if value in ('True','False'):
            return boolDict[value]
        else:
            return value
    
    def intConverter(value):
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
            
    def floatConverter(value):
        try:
            value = float(value)
            return value 
        except:
            return value

    def noneTypeConverter(value):
        if value == 'None':
            return None
        else:
            return value 
    


    # Logic for getting model parameters and setting (Custom Form)
    if getParams == 'Yes' and 'Yes' not in [dataChanges,trainModel,saveModel,predictModel]:

        with placeholder.form("hyperparam_form"):
            if not hasattr(st.session_state,'algoType') or st.session_state.algoType != algoType:
                st.session_state.hyperparams = {}
                model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]))
                modelParamDict = model.get_params()
            elif bool(st.session_state.hyperparams):
                hyperparams = st.session_state.hyperparams
                model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]),hyperparams)
                modelParamDict = model.get_params()
        
            st.write(f" :green[{algoType}] Hyperparameters")
            for key,value in modelParamDict.items():
                modelParamDict[key] = st.text_input(f"{key}",modelParamDict.get(key,value))
                modelParamDict[key] = floatConverter(modelParamDict[key])
                modelParamDict[key] = intConverter(modelParamDict[key])
                modelParamDict[key] = boolConverter(modelParamDict[key])
                modelParamDict[key] = noneTypeConverter(modelParamDict[key])
            submitted = st.form_submit_button("Update Hyperparameters")

            if submitted:
                updateHyperparameters(modelParamDict,algoType)
                placeholder.empty()
        


    # Logic for training a model
    if trainModel == 'Yes' and 'Yes' not in [getParams,dataChanges,saveModel,predictModel]:
        if not inputDataCheck(dataStore,modelData):
            return None
        elif hasattr(st.session_state,'hyperparams'):
            hyperparams = st.session_state.hyperparams
            model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]),hyperparams)
        elif not hasattr(st.session_state,'hyperparams') :
            model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]))

        trainedModel,X_test,y_test = modelTrain()
        st.session_state.model = cp.dumps(trainedModel).hex()

        st.title("Model Accuracy")
        st.text(trainedModel.score(X_test,y_test))



    # Data Transformations
    dataPreprocessingDict = {
        "StandardScaler": {"class":"StandardScaler","moduleName":"sklearn.preprocessing"},
        "MinMaxScaler": {"class":"MinMaxScaler","moduleName":"sklearn.preprocessing"},
        #"OneHotEncoder": {"module":"OneHotEncoder","package":"sklearn.preprocessing"},
    }

    
    if dataChanges == 'Yes' and inputDataCheck(dataStore,modelData) and 'Yes' not in [getParams,trainModel,saveModel,predictModel]:
            if not hasattr(st.session_state,"data"):
                getTrainAndTestData(new_session,str(modelData),dataStore)
            
            X_train,X_test,y_train,y_test = st.session_state.data
            
            dataTransformationForm = st.empty()
            with dataTransformationForm.form("transformData"):
                transformX = st.selectbox("Independent Variable Transformations",['None','StandardScaler','MinMaxScaler','Log-Transform'])
                transformY = st.selectbox("Dependent Variable Transformations",['None','Log-Transform'])
                submitted = st.form_submit_button("submit")
            
            if submitted:
                if transformX != 'None' and transformX != 'Log-Transform':
                    moduleName = dataPreprocessingDict[transformX]["moduleName"]
                    objectClass = dataPreprocessingDict[transformX]["class"]
                    dataPreprocessor = pythonObjectInstance(new_session,objectClass,moduleName)
                    scaledXtrain = dataPreprocessor.fit_transform(X_train)
                    scaledXtest = dataPreprocessor.fit_transform(X_test)
                    st.success(f"X_train Shape:{scaledXtrain.shape}, X_test Shape:{scaledXtest.shape}")

                
                if transformX == 'Log-Transform':
                    transformedXtrain = np.log(X_train)
                    transformedXtest = np.log(X_test)
                    st.success(f"X_train Shape:{transformedXtrain.shape}, X_test Shape:{transformedXtest.shape}")


                # if transformY != 'None' and transformY != 'Log-Transform':
                #     moduleName = dataPreprocessingDict[transformY]["moduleName"]
                #     objectClass = dataPreprocessingDict[transformY]["class"]
                #     dataPreprocessor = pythonObjectInstance(new_session,objectClass,moduleName)
                #     scaledYtrain = dataPreprocessor.fit_transform(y_train)
                #     scaledYtest = dataPreprocessor.fit_transform(y_test)
                #     st.success(scaledYtrain.shape)
                
                if transformY == 'Log-Transform':
                    transformedYtrain = np.log(y_train)
                    transformedYtest = np.log(y_test)
                    st.success(f"Y_train Shape:{transformedYtrain.shape}, Y_test Shape:{transformedYtest.shape}")


                st.session_state.data = [X_train,X_test,y_train,y_test]

                dataTransformationForm.empty()



    if predictModel == 'Yes' and 'Yes' not in [getParams,dataChanges,trainModel,saveModel]:
        if not inputDataCheck(dataStore,modelData) or dataStore == 'My Computer': # if dataset is not specified
            st.stop()
        if not hasattr(st.session_state,'data'):
            getTrainAndTestData(new_session,str(modelData),dataStore)
        elif hasattr(st.session_state,'hyperparams'):
            hyperparams = st.session_state.hyperparams
            model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]),hyperparams)
        elif not hasattr(st.session_state,'hyperparams') :
            model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]))
        predictionForm = st.empty()
        preDict = {key:'' for key in st.session_state.dataCols[0:-1]}

        with predictionForm.form('predict_here'):
            for key,value in preDict.items():
                preDict[key] = st.text_input(f"{key}",modelParamDict.get(key,value))
            submitted = st.form_submit_button("submit")
            
        if submitted:
          trainedModel,X_test,y_test = modelTrain()
          sampleData = np.array([int(feature) for key,feature in preDict.items()])
          sampleData = sampleData.reshape(1,len(sampleData))
          st.success(f'{trainedModel.predict(sampleData)}')
          predictionForm.empty() 
          


    if saveModel == 'Yes' and 'Yes' not in [getParams,dataChanges,trainModel,predictModel]:
        saveModelForm = st.empty()
        with saveModelForm.form('save_here'):
            # File or database
            locationType = st.selectbox('Save Method',['Snowflake Table'])
            locationName = st.text_input('Save Model As','')
            submitted = st.form_submit_button('submit')
        
        if submitted:
            data = pd.DataFrame({"Model":[st.session_state.model]})
            snowpark_df = new_session.write_pandas(data, "saved_models", auto_create_table=True, table_type="temporary")
            snowpark_df.write.mode("append").save_as_table("saved_models")



            saveModelForm.empty()
            st.success('submitted')

    return None



main()
