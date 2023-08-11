from snowflake.snowpark import Session
import streamlit as st
import cloudpickle as cp
from sklearn.model_selection import train_test_split
import xgboost
import time as t

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


   

    
    
    def modelInstance(session,modelName,moduleName):
        modelOutput = session.call("MLBuilder",str(modelName),str(moduleName))
        if not hasattr(st.session_state,'hyperparams') or st.session_state.hyperparams == {}:
            return cp.loads(bytes.fromhex(modelOutput)) 
        else:
            return cp.loads(bytes.fromhex(modelOutput)).set_params(**st.session_state.hyperparams)
    
    def getTrainAndTestData(session,tableName):
        # Load features
        df = session.table(tableName).to_pandas()
        X = df.iloc[:,:-1]
        y = df.iloc[:,-1:]

        # Split dataset into training and test
        return train_test_split(X, y, test_size=0.2, random_state = 42)
    
    
    #Sidebar
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
    
    
    
    #st.session_state.library = modelDict[predictionTask][algoType]


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

    
    
    def updateHyperparameters(hyperParamDict):
        st.success('Updated Hyperparameters')
            #Update model parameters
        #for key,value in hyperParamDict:    
        #    modelParamDict[key] = st.session_state.hyperparam_form[key]
        #    st.write(f"{key}: {modelParamDict[key]}")
    
    

    #Dictionary to hold new parameters
    modelParamDict = {} #Should I move this?
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
            else:
                st.text('something else')

        
            st.write(f" :green[{algoType}] Hyperparameters")
            for key,value in modelParamDict.items():
                modelParamDict[key] = st.text_input(f"{key}",modelParamDict.get(key,value))
            submitted = st.form_submit_button("Update Hyperparameters")

            if submitted:
                st.session_state.hyperparams.update(modelParamDict)
                st.session_state.algoType = algoType
                st.text(st.session_state.hyperparams)
                #st.success('We Did It!')
                #st.session_state.hyperparams = {}
                #st.text(st.session_state.hyperparams)
                placeholder.empty()
        

        
    
            
        
                 
        
        
      
    
    # Logic for training a model
    if trainModel:
        if dataStore == 'My Computer':
            st.error("Self Uploads Are Currently Not Supported", icon='☹')
        if dataStore == 'Snowflake' and modelData == '':
            st.error("Please Enter A Table To Build Your Model", icon='☹')
        else:
            model = modelInstance(new_session,str(algoType) ,str(modelDict[predictionTask][algoType]))
            X_train, X_test, y_train, y_test = getTrainAndTestData(new_session,str(modelData)) #Only Supports Snowflake data for now
            model.fit(X_train,y_train)
            st.title("Model Accuracy")
            st.text(model.score(X_test,y_test))
    
    if dataChanges:
        pass

    if predictModel:
        pass

    if saveModel:
        pass

    return None



main()
