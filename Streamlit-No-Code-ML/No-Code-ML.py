import streamlit as st
from sklearn.model_selection import train_test_split
import sklearn.metrics as metrics
import xgboost
import time as t
import numpy as np
import pandas as pd

email = ''
schema = ''


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
                         "Breast Cancer (Binary Classification)":"load_breast_cancer"
                     } 
                   }


   #================== Model Instance Functions =====================#

    
    #import module class
    def importName(moduleName,className):
        try:
            module = __import__(moduleName, globals(),locals(),[className])
        except ImportError:
            return None
        return vars(module)[className]
    
    
    # instantiate new ml model 
    @st.cache_resource  
    def modelInstance(modelName,moduleName,hyperparams={}):
        modelOutput = importName(str(moduleName),str(modelName))
        if hyperparams == {}:
            return modelOutput()
        else:
            return modelOutput().set_params(**hyperparams)
            

    def modelTrain():
        if hasattr(st.session_state,'data'):    
            X_train, X_test, y_train, y_test = st.session_state.data
            model.fit(X_train,y_train)
            return [model,X_test,y_test]
            
        else:
            X_train, X_test, y_train, y_test = getTrainAndTestData(str(modelData),dataStore)
            model.fit(X_train,y_train)
            return [model,X_test,y_test]
   #================== Data Related Functions =====================#

    # Data processors help perform statistics based transformations
    def pythonObjectInstance(className,moduleName):
        pythonObjectInst = importName(str(moduleName),str(className))
        return pythonObjectInst()
    
    def trainTestSplit(dataframe) -> None:
        X = dataframe.iloc[:,:-1]
        y = dataframe.iloc[:,-1:]
        st.session_state.data = train_test_split(X, y, test_size=0.2, random_state = 42) #parameterize in future
        st.session_state.dataCols = dataframe.columns
        return None
    
    
    def getTrainAndTestData(tableName,dataStore) -> list:
        if dataStore == 'My Computer':
            df = pd.read_csv(modelData)
            trainTestSplit(df)
            st.session_state.feature_names = df.columns
            return st.session_state.data
        
        if dataStore == 'Sklearn Dataset':
            dataInst = pythonObjectInstance(sklearnData['data'][modelData],sklearnData['module'])
            df = pd.DataFrame(np.column_stack((dataInst['data'],dataInst['target'])),
                            columns=[*dataInst['feature_names'],'target'])
            trainTestSplit(df)
            return st.session_state.data
        

    
    
    def inputDataCheck(modelData,dataStore,algoType) -> None:
        if not hasattr(st.session_state,"data"):
            getTrainAndTestData(str(modelData),dataStore)
            st.session_state.dataStore = dataStore 
            st.session_state.modelData = modelData
        if hasattr(st.session_state,"dataStore") and st.session_state.dataStore != dataStore:
            getTrainAndTestData(str(modelData),dataStore)
            st.session_state.dataStore = dataStore 
            st.session_state.modelData = modelData
        if hasattr(st.session_state,'modelData') and st.session_state.modelData != modelData:
            getTrainAndTestData(str(modelData),dataStore)
            st.session_state.dataStore = dataStore 
            st.session_state.modelData = modelData
        if not hasattr(st.session_state,'algoType') or st.session_state.algoType != algoType:
            st.session_state.hyperparams = {}
            model = modelInstance(str(algoType),str(modelDict[predictionTask][algoType]))
            modelParamDict = model.get_params()
        if bool(st.session_state.hyperparams):
            hyperparams = st.session_state.hyperparams
            model = modelInstance(str(algoType) ,str(modelDict[predictionTask][algoType]),hyperparams)
            modelParamDict = model.get_params()
        if not hasattr(st.session_state,'data'):
            getTrainAndTestData(str(modelData),dataStore)
        if hasattr(st.session_state,'hyperparams'):
            hyperparams = st.session_state.hyperparams
            model = modelInstance(str(algoType) ,str(modelDict[predictionTask][algoType]),hyperparams)
        if not hasattr(st.session_state,'hyperparams') :
            model = modelInstance(str(algoType) ,str(modelDict[predictionTask][algoType]))
        
        #return True
        
    
    
   #================== App frontend design =====================#
    
    
    st.sidebar.success('The Research Lab™')

    st.header(':green[No]-Code-ML')

    # Display the input values
    predictionTask = st.selectbox("Prediction Task",["Classification","Regression"])
    dataStore = st.selectbox("Data Location",['My Computer','Sklearn Dataset'])
    if dataStore == 'Sklearn Dataset':
        modelData = st.selectbox("Choose a Dataset",[i for i in sklearnData['data'].keys()])
    if dataStore == 'My Computer':
        modelData = st.file_uploader('Upload Data as CSV')
    algoType = st.selectbox("Algorithm Type", [i for i in modelDict[predictionTask].keys()]) 
    

    # Create Buttons for Setting Model Parameters, Model Training, and Data Transformations 
    paramsBtn,dataTransformBtn,trainBtn,predictBtn = st.columns([0.07,0.06,0.04,0.04],gap="small")
    with paramsBtn:
        getParams = st.selectbox('Set Model Params',['No','Yes'],index=0)
    with dataTransformBtn:
        dataChanges = st.selectbox('Transform Data',['No','Yes'],index=0)
    with trainBtn:
        trainModel = st.selectbox('Train',['No','Yes'],index=0)
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
    if getParams == 'Yes' and 'Yes' not in [dataChanges,trainModel,predictModel]:

        with placeholder.form("hyperparam_form"):
            if not hasattr(st.session_state,'algoType') or st.session_state.algoType != algoType:
                st.session_state.hyperparams = {}
                model = modelInstance(str(algoType),str(modelDict[predictionTask][algoType]))
                modelParamDict = model.get_params()
            if bool(st.session_state.hyperparams):
                hyperparams = st.session_state.hyperparams
                model = modelInstance(str(algoType) ,str(modelDict[predictionTask][algoType]),hyperparams)
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
    if trainModel == 'Yes' and 'Yes' not in [getParams,dataChanges,predictModel]:
        #if not inputDataCheck(dataStore,modelData):
        #    return None
        if hasattr(st.session_state,'hyperparams'):
            hyperparams = st.session_state.hyperparams
            model = modelInstance(str(algoType) ,str(modelDict[predictionTask][algoType]),hyperparams)
        elif not hasattr(st.session_state,'hyperparams') :
            model = modelInstance(str(algoType) ,str(modelDict[predictionTask][algoType]))
        #inputDataCheck(modelData,dataStore,algoType)

        trainedModel,X_test,y_test = modelTrain()
        st.session_state.model = trainedModel

        st.title("Model Accuracy")
        if predictionTask == 'Classification':
            report = metrics.classification_report(trainedModel.predict(X_test),y_test,output_dict=True)
            st.table(pd.DataFrame(report).T)
            
        else:
            st.text(trainedModel.score(X_test,y_test))



    # Data Transformations
    dataPreprocessingDict = {
        "StandardScaler": {"class":"StandardScaler","moduleName":"sklearn.preprocessing"},
        "MinMaxScaler": {"class":"MinMaxScaler","moduleName":"sklearn.preprocessing"},
    }

    
    if dataChanges == 'Yes' and 'Yes' not in [getParams,trainModel,predictModel]:
            if not hasattr(st.session_state,"data"):
                getTrainAndTestData(str(modelData),dataStore)
                st.session_state.dataStore = dataStore 
                st.session_state.modelData = modelData
            if hasattr(st.session_state,"dataStore") and st.session_state.dataStore != dataStore:
                getTrainAndTestData(str(modelData),dataStore)
                st.session_state.dataStore = dataStore 
                st.session_state.modelData = modelData
            if hasattr(st.session_state,'modelData') and st.session_state.modelData != modelData:
                getTrainAndTestData(str(modelData),dataStore)
                st.session_state.dataStore = dataStore 
                st.session_state.modelData = modelData
            #inputDataCheck(modelData,dataStore,algoType)

            
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
                    moduleName = dataPreprocessingDict[transformX]["moduleName"]
                    objectClass = dataPreprocessingDict[transformX]["class"]
                    dataPreprocessor = pythonObjectInstance(objectClass,moduleName)
                    X_train = dataPreprocessor.fit_transform(X_train)
                    X_test = dataPreprocessor.fit_transform(X_test)
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



    if predictModel == 'Yes' and 'Yes' not in [getParams,dataChanges,trainModel]:
        if not hasattr(st.session_state,'data'):
            getTrainAndTestData(str(modelData),dataStore)
        elif hasattr(st.session_state,'hyperparams'):
            hyperparams = st.session_state.hyperparams
            model = modelInstance(str(algoType) ,str(modelDict[predictionTask][algoType]),hyperparams)
        elif not hasattr(st.session_state,'hyperparams') :
            model = modelInstance(str(algoType) ,str(modelDict[predictionTask][algoType]))
        if hasattr(st.session_state,'modelData') and st.session_state.modelData != modelData:
            getTrainAndTestData(str(modelData),dataStore)
            st.session_state.dataStore = dataStore 
            st.session_state.modelData = modelData
        


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
          


    
    return None



main()
