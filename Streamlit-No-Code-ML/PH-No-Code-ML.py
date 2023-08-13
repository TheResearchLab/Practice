from snowflake.snowpark import Session
import streamlit as st
import cloudpickle as cp
from sklearn.model_selection import train_test_split
import xgboost
import time as t
import numpy as np

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


   #================== Model Instance Functions =====================#

    
    # instantiate new ml model 
    def modelInstance(session,modelName,moduleName):
        modelOutput = session.call("InstantiatePythonObject",str(modelName),'Model',str(moduleName))
        if not hasattr(st.session_state,'hyperparams') or st.session_state.hyperparams == {}:
            return cp.loads(bytes.fromhex(modelOutput))
        else:
            return cp.loads(bytes.fromhex(modelOutput)).set_params(**st.session_state.hyperparams)
            


   #================== Data Related Functions =====================#

    def getTrainAndTestData(session,tableName,dataStore) -> list:
        if dataStore == 'My Computer':
            return None
        
        if dataStore == 'Snowflake':
            df = session.table(tableName).to_pandas()
            X = df.iloc[:,:-1]
            y = df.iloc[:,-1:]
            return train_test_split(X, y, test_size=0.2, random_state = 42) # these should get parameterized

        # Split dataset into training and test
    
    def inputDataCheck(dataStore,modelData) -> bool:
        if dataStore == 'My Computer':
            st.error("Self Uploads Are Currently Not Supported", icon='☹')
            return False
        if dataStore == 'Snowflake' and modelData == '':
            st.error("Please Enter A Table To Build Your Model", icon='☹')
            return False
        else:
            return True
        
    def dataPreprocessorInstance(session,className,moduleName):
        dataPreprocessor = session.call("InstantiatePythonObject",str(className),'Data Preprocessor',str(moduleName))
        return cp.loads(bytes.fromhex(dataPreprocessor))



    
    
    
   #================== App frontend design =====================#
    
    
    st.sidebar.success('© 2023 The Top Secret Data Science Club™')

    st.header(':green[PH]-No-Code-ML')

    # Display the input values
    predictionTask = st.selectbox("Prediction Task",["Classification","Regression"])
    dataStore = st.selectbox("Data Location",['Snowflake','My Computer'])
    if dataStore == 'Snowflake':
        modelData =  st.text_input("Table/View Name")
    else:
       modelData = st.file_uploader('Upload Data')
    algoType = st.selectbox("Algorithm Type", [i for i in modelDict[predictionTask].keys()]) 
    

    # Create Buttons for Setting Model Parameters, Model Training, and Data Transformations 
    paramsBtn,dataTransformBtn,trainBtn,predictBtn,saveBtn = st.columns([0.07,0.06,0.04,0.04,0.04],gap="small")
    with paramsBtn:
        getParams = st.selectbox('Manually Set Params',['No','Yes'],index=0)
    with dataTransformBtn:
        dataChanges = st.selectbox('Transform Data',['No','Yes'],index=0)
    with trainBtn:
        trainModel = st.button('💪 Train')
    with predictBtn:
        predictModel = st.button('🧐 Predict')
    with saveBtn:
        saveModel = st.button('📝 Save')

    
    
    def updateHyperparameters(modelParamDict,algoType) -> None:
        st.session_state.hyperparams.update(modelParamDict)
        st.session_state.algoType = algoType
        st.text(st.session_state.hyperparams)
        st.success('Updated Hyperparameters')
        
    
    

    

   #================== App Event Listeners =====================#

    #Dictionary to hold new parameters
    modelParamDict = {}
    placeholder = st.empty() 

    # Logic for getting model parameters and setting (Custom Form)
    if getParams == 'Yes':

        with placeholder.form("hyperparam_form"):
            if not hasattr(st.session_state,'algoType') or st.session_state.algoType != algoType:
                st.session_state.hyperparams = {}
                model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]))
                modelParamDict = model.get_params()
            elif bool(st.session_state.hyperparams):
                model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]))
                modelParamDict = model.get_params()

        
            st.write(f" :green[{algoType}] Hyperparameters")
            for key,value in modelParamDict.items():
                modelParamDict[key] = st.text_input(f"{key}",modelParamDict.get(key,value))
            submitted = st.form_submit_button("Update Hyperparameters")

            if submitted:
                updateHyperparameters(modelParamDict,algoType)
                placeholder.empty()
        


    # Logic for training a model
    if trainModel:
        if not inputDataCheck(dataStore,modelData):
            return None
        else:
            model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]))
            # if there is data in cache get 
            X_train, X_test, y_train, y_test = st.session_state.data #Only Supports Snowflake data for now
            model.fit(X_train,y_train)
            st.title("Model Accuracy")
            st.text(model.score(X_test,y_test))

    dataPreprocessingDict = {
        "StandardScaler": {"class":"StandardScaler","moduleName":"sklearn.preprocessing"},
        "MinMaxScaler": {"class":"MinMaxScaler","moduleName":"sklearn.preprocessing"},
        #"OneHotEncoder": {"module":"OneHotEncoder","package":"sklearn.preprocessing"},
    }

    
    if dataChanges == 'Yes' and inputDataCheck(dataStore,modelData):
            if not hasattr(st.session_state,"data"):
                st.session_state.data = getTrainAndTestData(new_session,str(modelData),dataStore)
            
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
                    dataPreprocessor = dataPreprocessorInstance(new_session,objectClass,moduleName)
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
                #     dataPreprocessor = dataPreprocessorInstance(new_session,objectClass,moduleName)
                #     scaledYtrain = dataPreprocessor.fit_transform(y_train)
                #     scaledYtest = dataPreprocessor.fit_transform(y_test)
                #     st.success(scaledYtrain.shape)
                
                if transformY == 'Log-Transform':
                    transformedYtrain = np.log(y_train)
                    transformedYtest = np.log(y_test)
                    st.success(f"Y_train Shape:{transformedYtrain.shape}, Y_test Shape:{transformedYtest.shape}")


                st.session_state.data = [X_train,X_test,y_train,y_test]

                dataTransformationForm.empty()



    if predictModel:
        pass

    if saveModel:
        pass

    return None



main()
