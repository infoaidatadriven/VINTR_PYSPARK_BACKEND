from flask import Blueprint,request, render_template, send_from_directory,Flask
from flask_cors import CORS
import pandas as pd
import json
from werkzeug.utils import secure_filename
from dataLineage import EntryDataLineage ,dataLineageforcolumn  
from dataQuality import checkQuality,checkDataQuality,savequlaitychecktodb
from CorrelationFunctions import compute_associations
from common_PC_Function import *

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score,confusion_matrix,classification_report,f1_score

from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import RidgeClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.ensemble import BaggingClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn import linear_model
import pickle
from sklearn.preprocessing import LabelEncoder, StandardScaler
import collections

DataProfile = Blueprint('DataProfile',__name__)


# common api for both profile and clean module

app_1 = Flask(__name__)
CORS(app_1)
app_1.config['UPLOAD_EXTENSIONS'] = ['.csv', '.xlsx', '.xls', '.json', '.xml']
app_1.config['MAX_CONTENT_LENGTH'] = 10000* 1024 * 1024


@DataProfile.route('/api/EditSourcePC', methods=['POST'])
def EditSourcePC():
    """
    Edit Source for Profile and Data Cleaning Modules
    """
    content = request.get_json()
    action = content["action"]
    old_source = content["old_source"]
    new_source = content["new_source"]
    db = content["db"]
    if db == "profile":
        data = json.load(open("profiledb.json","r"))
    elif db == "clean":
        data = json.load(open("cleandb.json","r"))
    ### Convert JSON data to list of dictionaries
    proc_data = []
    for row in data["SourceDetailsList"]:
        # print(row,'\n')
        proc_data.append(row)
    df = pd.DataFrame(data = proc_data)
    print(df)
    index = df[(df["sourceDataName"] == old_source["sourceDataName"]) &
                   (df["sourceId"] == old_source["sourceId"]) &
                   (df["sourceCategory"] == old_source["sourceCategory"])].index.to_list()
    print("Row Index matching condition:", index)
    if len(index) == 0:
        return {"QueryError" : "One or more details in old_source not found."}
    if action == "remove":
        old_file = list(df.iloc[index]["templateSourcePath"])[0]
        print("Removing Old File:",old_file)
        os.remove(old_file)
        df.drop(index, axis = 0, inplace = True)
    # print("Profile DB dataframe:",df)
    elif action == "edit":
        # if (old_source["sourceFileName"] != new_source["sourceFileName"] ) and (old_source["sourceDataName"] != new_source["sourceDataName"] ):
        #     old_template_path = list(df.iloc[index]["templateSourcePath"])[0]
        #     ps_sourceID, file_ext = tuple(old_template_path.split(old_source["sourceFileName"]))
        #     new_template_path = ps_sourceID + new_source["sourceFileName"] + file_ext
        #     print('Old FileName:',old_template_path, "\nNew FileName:",new_template_path)
        #     os.replace(old_template_path, new_template_path )
        #     new_source['templateSourcePath'] = new_template_path
        #     new_source['sourceId'] = ps_sourceID
        new_source_cols = list(new_source.keys())
        new_source_values = list(new_source.values())
        ## Replace old data in row with new values
        df.loc[index,new_source_cols] = new_source_values
        
    out_data = {"SourceDetailsList": df.to_dict(orient = "records")}
    
    if db == "profile":
        json.dump(out_data, open("profiledb.json","w"), default=str)
        new_source["source"]="profile"
        new_source['originalsourceId']=old_source["sourceId"]
        editCleaningSourceFromProfile("profile",old_source["sourceId"],sourcedetails=new_source)    
    elif db == "clean":
        json.dump(out_data, open("cleandb.json","w"), default=str)
        new_source["source"]="clean"
        new_source['originalsourceId']=old_source["sourceId"]
        editProfileSourceFromcleaning("clean",old_source["sourceId"],sourcedetails=new_source)    
    editcommondb(old_source["sourceId"],sourcedetails=new_source)    
    print("Source Details updated successfully in {} DB".format(db))
    # messagel = "File Received in "+ db
    # EntryDataLineage(path,newSourceId,  sourceDataName,newSourceId, str(datetime.datetime.now()),"DQM",messagel)
     
    return out_data

@DataProfile.route('/api/configureSourcePC', methods=['POST'])
def configureSourcePC():
    """
    Create New Source for Profile and Data Cleaning Modules
    """
    uploaded_files = request.files.getlist("file[]")
    content = json.loads(request.form.get('data'))
    sourcedetails= content['SourceSettings']
    
    selectedColumns=[]
    db = content["db"]
    data_dict={}
    #source related details
    sourceDataName = sourcedetails['sourceDataName']
    sourceFilename = sourcedetails['sourceFileName']
    sourceCategory = sourcedetails['sourceCategory']
    if ("department" in sourcedetails ):
        department = sourcedetails["department"]
    else:
        department=[]
    if ("frequency" in sourcedetails ):
       frequency = sourcedetails["frequency"]
       expectedUploadDate= sourcedetails["expectedUploadDate"]
       expecteduploadTime= sourcedetails["expecteduploadTime"]
       isOriginalSource=sourcedetails["isOriginalSource"]
    else:
        sourcedetails["frequency"]={}
        sourcedetails["expectedUploadDate"]=""
        sourcedetails["expecteduploadTime"]=""
        sourcedetails["isOriginalSource"]="No"
    existing_sources_list = listSourceNamesCommon()
    print('Source Names List:', existing_sources_list)
    if sourceDataName in existing_sources_list:
        content = {}
        content['errorMsg']= 'The source name already exists'
        content['errorflag']= 'True'
        content['errorCode']= '101'
        return json.dumps(content, default=str)
    else:
        if db == "profile":
            data = json.load(open("profiledb.json","r"))
            newSourceId = sourceDataName[0] + sourceDataName[-1] + '_PS'+ str(int(getPC_SourceMaxId(sourceCategory, db)+1))
        elif db == "clean":
            data = json.load(open("cleandb.json","r"))
            newSourceId = sourceDataName[0] + sourceDataName[-1] + '_CS'+ str(int(getPC_SourceMaxId(sourceCategory, db)+1))
        sourceFilename = newSourceId + '_' + sourceFilename
        isFromDQM="NO"
        if "sourcePath" in sourcedetails.keys():
            print("For DQ to Data Cleaning Source Addition")
            path = sourcedetails["sourcePath"]
            newSourceId = sourcedetails["sourceId"]
            isFromDQM="YES"
        else:
            for file in uploaded_files:
                filename = secure_filename(file.filename)
                print("FileName:",filename)
                file_ext = os.path.splitext(filename)[1]
                if file_ext in app_1.config['UPLOAD_EXTENSIONS'] :
                    file.save(sourceFilename)
                    fileize= os.path.getsize(sourceFilename)
                    filesize = fileize/(1024*1024)
                    path=sourceFilename
                    try:
                        if(file_ext==".csv"):
                            try:
                                #print("in csv")
                                str_dem = find_delimiter(path)
                                #print("delimiter",str_dem)
                                if(str_dem =="True"):
                                    if filesize>10:                    
                                        chunksize = 50
                                        df= getchunkforPreview(path,chunksize)
                                    else:
                                        df= df_from_path(path)
                                else:
                                   raise pd.errors.ParserError
                            except(pd.errors.ParserError):
                                data_dict={}
                                content['errorMsg']= 'Upload File Not A Valid CSV File'
                                content['errorflag']= 'True'
                                content['errorCode']= '105'
                                return json.dumps(content, default=str)
                        # df = df_from_path(path)
                        else:
                            df = df_from_path(path)   
                        # df = df_from_path(path)
                        if any ('Unnamed' in col for col in df.columns.values.tolist()):
                            print('Missing Header Columns')
                            error_msg ="Missing Header column"
                            raise pd.errors.ParserError
                        #28/04/2022- Hari #97
                        selectedColumns=  list(df.columns.values)
                        for k in selectedColumns:
                            if (df[k].nunique() == 0):
                                error_msg ="The entire column '{}' contains no value".format(k)
                                raise pd.errors.EmptyDataError
                        
                    except (pd.errors.ParserError, pd.errors.EmptyDataError):
                        content = {}
                        content['errorMsg']= 'Data Parsing Failed. {}'.format(error_msg)
                        content['errorflag']= 'True'
                        content['errorCode']= '105'
                        return json.dumps(content, default=str)
        dli= list(df.columns.values)
        sourcedetails['templateSourcePath']= path
        sourcedetails['availableColumns'] = dli
        sourcedetails['sourceId']= newSourceId
        sourcedetails['sqlLitePath']='sqlite:///'+newSourceId + '_'+ sourceDataName+'.db'
        try:
            fileize= os.path.getsize(path)
            filesize = fileize/(1024*1024)
            if filesize>15:
                SaveFileAsSQlLite(path,'sqlite:///'+newSourceId + '_'+ sourceDataName+'.db')
                sourcedetails['IsSqlLiteSource']='Yes'
            else:
                sourcedetails['IsSqlLiteSource']='No'
        except:
            sourcedetails['IsSqlLiteSource']='No'
        sourcedetails['department']= department
        sourcedetails["sourcePath"]= path
        sourcedetails["isFromDQM"]= isFromDQM
        sourcedetails["CleanedFilesLog"]= []
        sourcedetails["rules"]= []
    SourceList=  data['SourceDetailsList']
    SourceList.append(sourcedetails)
    data['SourceDetailsList'] = SourceList
    newsourcedetails= sourcedetails
    newsourcedetails['sourceId']= newSourceId
    newsourcedetails['originalsourceId']= newSourceId
    if db == "profile":
        json.dump(data, open("profiledb.json","w"), default=str)
        addCleaningSourceFromProfile("profile",newsourcedetails)
        updatecommondb("profile",sourcedetails)
    elif db == "clean":
        json.dump(data, open("cleandb.json","w"), default=str)
        addProfileSourceFromcleaning("clean",newsourcedetails)
        updatecommondb("clean",sourcedetails)
    content['errorMsg']= ''
    content['errorflag']= 'False'
    messagel = "File Received in "+ db
    EntryDataLineage(path,newSourceId,  sourceDataName,newSourceId, str(datetime.datetime.now()),db,messagel,selectedColumns)
    EntryDataLineage(path,newSourceId,  sourceDataName,newSourceId, str(datetime.datetime.now()),db,'File storage',selectedColumns)
      
    return json.dumps(content, default=str)

@DataProfile.route('/api/configureSourcePC_pyspark', methods=['POST'])
def configureSourcePyspark():
    uploaded_files = request.files.getlist("file[]")
    content = json.loads(request.form.get('data'))  

    for file in uploaded_files:
            filename = secure_filename(file.filename)
            if filename != '':
                    file_ext = os.path.splitext(filename)[1]
                    if file_ext in ['.csv', '.xlsx', '.xls', '.json', '.parquet'] :
                        sourcePath = filename
                        file.save(sourcePath)

    data = json.load(open("pysparkProfiledb.json","r"))
    content['sourceId'] = len(data['SourceDetailsList']) + 1
    content['sourcePath'] = sourcePath
    data['SourceDetailsList'].append(content)
    json.dump(data, open("pysparkProfiledb.json","w"), default=str)
    return json.dumps(content, default=str)

@DataProfile.route('/api/GetAllPySparkSourceList', methods=['POST'])
def GetAllPySparkSourceList():
    data = json.load(open("pysparkProfiledb.json","r"))
    jsonString = json.dumps(data, default=str)
    return jsonString

@DataProfile.route('/api/GetAllProfiledBSourceList', methods=['POST'])
def GetAllProfiledBSourceList():
    content = request.get_json()   
    print(content) 
    userName=content['userName']
    userCategory=content['userCategory']
    userDepartment= content['department']
    '''API to retrieve complete profiledb json db file'''
    data = json.load(open("profiledb.json","r"))

    profilesingleList=[]
    for listitem in data['SourceDetailsList']:
        if isprofileSourceValid(userName,userCategory,userDepartment,listitem) :
            listforUploadHistory=[]
            sourceId=listitem["sourceId"]
            LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB_profile(sourceId))
            print(LaunchEntityRaw)
            listforUploadHistory= LaunchEntityRaw['EntireSourceObj']
            listforUploadHistorylist=[listforUploadHistory]
            uploadSourceList=[]
            datedHistory=[]
            recentsourceUpload={}
            for objldb in listforUploadHistorylist:
                    dateList=[]
                    dateLitnew=[]
                    
                    dateLitnew=(([((datetime.datetime.strptime(d['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")).date()) for d in objldb["uploads"] ]))                
                    test_list = list(set(dateLitnew))
                    test_list = sorted(test_list) 
                    # print('date list')
                    # print(test_list)
                    for eachdate in test_list:                    
                        datelist= [d for d in objldb["uploads"] if (datetime.datetime.strptime(d["uploadDate"],"%Y-%m-%dT%H:%M:%S.%fZ")).date()==eachdate] 
                        #li= [d for d in datelist if (d["isCleanedSource"])=="YES"] 
                        dateuploadList={}
                        dateuploadList['uploadDate'] =eachdate
                        uploadLIst=[]
                        datelistnew=datelist
                        if len(datelist)>1:
                            maxidnew=getmaxuploadId_profile(datelist,sourceId)
                            maxid=sourceId+'U'+ str(int( maxidnew))
                            print(maxid)
                            datelistnew=[d for d in datelist if d["uploadId"]==maxid]
                        for objldb1 in datelistnew:
                                detailsDict={}                             
                                uploadSourceDetail= {}
                                uploadSourceDetail['uploadId'] =objldb1["uploadId"]
                               
                               
                                uploadSourceDetail['uploadDate'] =objldb1["uploadDate"]
                                uploadSourceDetail['uploadTime'] =objldb1["uploadTime"]
                                uploadSourceDetail['sourceFileName']=objldb1["sourceFileName"]
                                uploadSourceDetail['templateSourcePath']=objldb1["sourceFileName"]
                                uploadSourceDetail['sourceFilepath']=objldb1["sourceFileName"]
                                detailsDict=uploadSourceDetail
                               
                                
                                uploadSourceList.append(uploadSourceDetail)
                                uploadLIst.append(detailsDict)
                                recentsourceUpload=uploadSourceDetail
                        dateuploadList['uploadDetails'] =uploadLIst
                        datedHistory.append(dateuploadList)
                    

            #listitem["UploadsHistory"]=listforUploadHistory
            listitem["UploadsHistory"]= uploadSourceList
            listitem["DatedUploadHistory"]=datedHistory
            listitem['recentsourceUpload'] = recentsourceUpload
            profilesingleList.append(listitem)
            

    data={}
    data['SourceDetailsList'] = profilesingleList
    jsonString = json.dumps(data, default=str)
    return jsonString
    


def getmaxuploadId_profile(sourceUploadlist,sourceId):
    
    IDList=[0]
    for obj1 in sourceUploadlist:
        IDList.append(obj1["uploadId"][(len(sourceId)+1):] )
    dli2 = [int(s) for s in IDList]
    return (str(max(dli2)))

@DataProfile.route('/api/profile', methods=['POST']) #GET requests will be blocked
def profileAnalysis():
    content = request.get_json()
    sourcepath= content['sourcepath']

    outputdata = json.load(open("ProfileResults.json","r"))
    OAnalysisList=  outputdata['Results']
    outputResData={}
    LDBDataList=[d for d in OAnalysisList if (d["sourcepath"])==sourcepath]
    if len(LDBDataList)>0:
        for eachitem in LDBDataList:
            filename=eachitem["results"]+'.json'
            rsult={}

            if os.path.isfile(filename):
                with open(filename, 'r') as openfile:
                    json_object = json.load(openfile)
                    return json.dumps(json_object, default=str)
            # rsult = json_object
            # jsonString = json.dumps(eachitem["results"], default=str)
            # return jsonString
    df = df_from_path_profile(sourcepath)
    
    selectedColumns=  list(df.columns.values)
    df_meta = pd.DataFrame(df.dtypes).reset_index()
    df_meta.columns = ["column_name", "dtype"]
    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
    conFeat = df_meta[df_meta.dtype == "int64"].column_name.tolist() + df_meta[df_meta.dtype == "float64"].column_name.tolist()
    print("conFeat:",conFeat)
    dateFeat = df_meta[df_meta.dtype == "datetime64[ns]"].column_name.tolist() + df_meta[df_meta.dtype == "datetime"].column_name.tolist()
    print("DateTime Features:", dateFeat)
    catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
    print("catFeat:",catFeat)
    ListResult=[]
    if(len(conFeat)>0):
        NumericCorrMatrix = GetCorrelationMatrix(df, 'numeric','pearson')
    if(len(catFeat)>0):
        CateoricalCorrMatrix = GetCorrelationMatrix(df, 'categorical' ,'theils_u')
    catalogueresults=getcatalogueforcolumns(selectedColumns)
    print("Starting the qulaity check")
    qualityresult= checkDataQuality(df.head(1000),selectedColumns)
    print("completed the qulaity check")
    c=savequlaitychecktodb(qualityresult,sourcepath)
    for k in selectedColumns:
        catalogueresult =  [x for x in catalogueresults if x['Column'] ==k]
        
        if k in conFeat:
            df_describe_continuous={}
            if (df[k].nunique()!=0):
                df_describe_continuous = forNumericType([k], df)
                # print("\ndf_describe_continous:",df_describe_continuous)
                ResMatrix=sorted_corr_matrix_per_col(NumericCorrMatrix,k)
                df_describe_continuous['correlationSummary']=ResMatrix
                dcatres=[]
                for eachresult in catalogueresult:
                   
                   eachresult['datalineage']=dataLineageforcolumn(k)
                   dcatres.append(eachresult)
                df_describe_continuous['datacatalogue']=dcatres
                df_describe_continuous['dq']=[x for x in qualityresult if x['ColumnName'] ==k][0]   
                dqvalidityResult= [x for x in qualityresult if x['ColumnName'] ==k][0]
                outliervalidity=0 
                for y in dqvalidityResult['detail']['Validity']['info']:
                    outliervalidity=outliervalidity+int(y['OutlierCount'])
                att=df_describe_continuous['attributeSummary']
                
                att['invalid'] =outliervalidity
                df_describe_continuous['attributeSummary']=att    
            ListResult.append(df_describe_continuous)
        if k in catFeat:
            df_describe_categorical={}
            if(df[k].nunique()!=0):
                df_describe_categorical = forStringType([k], df)
                if (str(mask_val["unique_values"]).contains("L") for mask_val in df_describe_categorical['maskAnalysis']) and (df_describe_categorical['attributeSummary']['dataType'] == "Numeric"):
                    print("\n Column for Outlier_Len_Analysis:",k, "DataType:", df_describe_categorical['attributeSummary']['dataType'])
                    df_describe_categorical['outlier_len_statistics'] = outlier_len_stats_numeric_cols(df_describe_categorical['maskAnalysis'])
                ResMatrix=sorted_corr_matrix_per_col(CateoricalCorrMatrix,k)
                # print("\n df_describe_categorical:",df_describe_categorical)
                df_describe_categorical['correlationSummary'] = ResMatrix
                
                dcatres=[]
                for eachresult in catalogueresult:
                   print(k)
                   print(eachresult)
                   eachresult['datalineage']=dataLineageforcolumn(k)
                   dcatres.append(eachresult)
                df_describe_categorical['datacatalogue']=dcatres
                df_describe_categorical['dq']=[x for x in qualityresult if x['ColumnName'] ==k][0]  
            ListResult.append(df_describe_categorical)
        if k in dateFeat:
            df_describe_datetime = forDateTimeType(k, df)
            dcatres=[]
            for eachresult in catalogueresult:
                   print(k)
                   print(eachresult)
                   eachresult['datalineage']=dataLineageforcolumn(k)
                   dcatres.append(eachresult)
            df_describe_datetime ['datacatalogue']=dcatres
            df_describe_datetime ['dq']=[x for x in qualityresult if x['ColumnName'] ==k][0]  
            ListResult.append(df_describe_datetime)
    resdata={}
    resdata['profile'] = ListResult
    nr_duplicates = len(df[df.duplicated()])
    resdata['nr_duplicates'] = nr_duplicates
    resdata['nr_totalrecords'] = len(df)
    resdata['nr_totalcols']=len(df.columns)
    inputdata = json.load(open("ProfileResults.json","r"))
    AnalysisList=  inputdata['Results']
    
    content['sourcepath']= sourcepath
    prId=getDBProfileId()
    content['results']="PR_"+prId
    content['ProfileRID']=prId
    AnalysisList.append(content)
    inputdata['Results'] = AnalysisList
    
    saveResultsets(resdata,"PR_"+prId)
    json.dump(inputdata, open("ProfileResults.json","w"), default=str)
    jsonString = json.dumps(resdata, default=str)
    return jsonString

@DataProfile.route('/api/anonymize', methods=['POST']) 
def anonymize():
    content = request.get_json()
    sourcepath= content['sourcepath']
    df = df_from_path_uploadSource(sourcepath)
    selected_cols= content['selected_cols']
    with open('selected_cols.txt', 'w') as file2:
        file2.write('\n'.join(selected_cols))
    catg_features = [col for col in selected_cols  if df[col].dtype == 'object']
    cont_features = [col for col in selected_cols if df[col].dtype != 'object']

    df[catg_features] = df[catg_features].astype(str)
    df[cont_features] = df[cont_features].astype(float)
    # impute missing values for continuous & categorical columns
    for col in catg_features:
        df[col].fillna(df[col].mode()[0], inplace=True)
    for col in cont_features:
        df[col].fillna(0, inplace=True)
    # Create a dictionary to store the encoding for each column
    encoders = {}
    scaler = StandardScaler()
    if cont_features != []:
        df[cont_features]=scaler.fit_transform(df[cont_features])
    # Encode all categorical columns using label encoding        
    for col in catg_features: 
        if df[col].dtype == 'object':
            encoders[col] = LabelEncoder()
            df[col] = pd.Series(encoders[col].fit_transform(df[col])) 

        # Store the encoded data as a CSV file
    df.to_csv('encoded_data.csv', index=False)
# Store the encoders dictionary using pickle
    with open('encoders.pkl', 'wb') as file:
        pickle.dump(encoders, file)
# Save the StandardScaler object
    with open('scaler.pkl', 'wb') as file1:
        pickle.dump(scaler, file1)
    df_json = df.to_json(orient='records')
    return df_json 


@DataProfile.route('/api/getCorrMatrix', methods=['POST']) #GET requests will be blocked
def getCorrMatrix():
    content = request.get_json()
    sourcepath= content['sourcepath']
    cols_data_type = content['cols_data_type']
    method = content['method']
    df = df_from_path(sourcepath)
    if content['cols_data_type'] == "":
        cols_data_type = 'mixed'
    numeric_cols = list(df.select_dtypes(include='number').columns)
    print(numeric_cols)
    cat_cols = list(df.select_dtypes(include=['category','object']).columns)
    if cols_data_type == 'numeric' and len(numeric_cols) == 0: ## if there are no numeric columns re-assign defaults
        print('No Numeric Columns detected. Reassigning cols_data_type to categorical')
        cols_data_type == 'categorical'
        method = 'theils_u'
    if cols_data_type == 'categorical' and len(cat_cols) == 0: ## if there are no categorical columns re-assign defaults
        print('No Categorical Columns detected. Reassigning cols_data_type to numeric')
        cols_data_type == 'numeric'
        method = 'pearson'
    if cols_data_type == 'numeric' or len(cat_cols) == 0:
        df_clean = df[numeric_cols]
        df_clean.dropna(inplace = True, axis = 'columns',  thresh = 1)
        if method == 'pearson':
            corr_matrix = df_clean.corr(method='pearson')
        elif method == 'kendall':
            corr_matrix = df_clean.corr(method='kendall')
        elif method == 'spearman':
            corr_matrix = df_clean.corr(method='spearman')
    if cols_data_type == 'categorical' or len(numeric_cols) == 0:
        df_clean = df[cat_cols]
        if method == 'theils_u':
            print('Computing Categorical Correlation Using Theils_U method')
            corr_matrix = compute_associations(df_clean, nominal_columns='all', theil_u=True, nan_strategy = 'drop_samples')
        elif method == 'cramers_v':
            print('Computing Categorical Correlation Using Cramers_V method')
            corr_matrix = compute_associations(df_clean, nominal_columns='all', theil_u=False, nan_strategy = 'drop_samples')
    if cols_data_type == 'mixed':
        mixed_cols = numeric_cols + cat_cols
        print("Mixed Cols:", mixed_cols)
        corr_matrix = compute_associations(df[mixed_cols], nominal_columns='auto', theil_u = False, bias_correction=False)#, nan_strategy = 'drop_samples')
    corr_matrix_final = corr_matrix.where(pd.notnull(corr_matrix), 0)
    resdata = corr_mat_highcharts(corr_matrix_final)
    jsonString = json.dumps(resdata, default=str)
    return jsonString

@DataProfile.route('/api/getTargetColumns', methods=['POST']) 
def getTargetColumns():
    content = request.get_json()
    sourcepath= content['sourcepath']
    df = df_from_path(sourcepath)

    # convert the date and time columns to DateTime type
    df = force_datetime_conversion(df)

    catg_features = [col for col in list(df.columns) if df[col].dtype == 'object']
    column_dict = {}
    for col in df.columns.tolist():
        if df[col].nunique() <= 10:
            column_dict[col] = df[col].nunique()
    sorted_column_dict = dict(sorted(column_dict.items(), key=lambda x:x[1]))
    
    jsonString = json.dumps(sorted_column_dict, default=str)
    return jsonString

@DataProfile.route('/api/getModelAccuracy', methods=['POST']) 
def getModelAccuracy():
    content = request.get_json()
    sourcepath= content['sourcepath']
    target_col = content['target_column']
    df = df_from_path(sourcepath)
    # df[target_col] = df[target_col].astype("string")

    # convert the date and time columns to DateTime type
    df = force_datetime_conversion(df)

    catg_features = [col for col in list(df.columns) if df[col].dtype == 'object' if col not in target_col]
    cont_features = [col for col in list(df.columns) if df[col].dtype != 'object' if col not in target_col]

    # impute missing values for continuous & categorical columns
    for col in catg_features:
        df[col].fillna(df[col].mode()[0], inplace=True)
    for col in cont_features:
        df[col].fillna(0, inplace=True)

    # Label encode the categorical columns
    for col in catg_features:
        if df[col].dtypes == 'object':
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col])

    df[target_col] = le.fit_transform(df[target_col])

    # Feature selection
    X = df.drop(target_col, axis=1)
    y = df[target_col]

    # Feature scaling
    std_scaling = StandardScaler()
    X = std_scaling.fit_transform(X)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=21)
    
    models = {
        'LogisticRegression' : LogisticRegression(),
        'Ridge_Classifier' : RidgeClassifier(),
        'DecisionTree_Classifier' : DecisionTreeClassifier(),
        'SupportVector_Classifier' : SVC(),
        'GaussianNaiveBayes': GaussianNB(),
        'KNeighbors_Classifier' : KNeighborsClassifier(),
        'RandomForest_Classifier' : RandomForestClassifier(),
        'XG_Boost_Classifier' : XGBClassifier(),
        'Stochastic_Gradient_Descent_Classifier' : SGDClassifier(),
        'Bagging_Classifier' :BaggingClassifier(),
        'AdaBoost_Classifier' :AdaBoostClassifier(),
        'GradientBoosting_Classifier' : GradientBoostingClassifier()
        }
    scores_dict = {}
    for key in models:
        m = models[key]
        m.fit(X_train,y_train)
        y_pred= m.predict(X_test)
        scores_dict[key] = {
            'Accuracy': round(accuracy_score(y_test, y_pred) * 100, 2)
            # 'F1 Score': round(f1_score(y_test, y_pred) * 100, 2),
            }
    scores_df = pd.DataFrame(scores_dict)
    scores_df_t = scores_df.T
    scores_df_t.sort_values(by='Accuracy', ascending = False, inplace=True)
    resdata = scores_df_t['Accuracy'].to_dict()
    jsonString = json.dumps(resdata, default=str)
    return jsonString

@DataProfile.route('/api/getModelimportances', methods=['POST']) 
def getModelimportances():
    content = request.get_json()
    sourcepath= content['sourcepath']
    target_col = content['target_column']
    df = df_from_path_uploadSource(sourcepath)
    catg_features = [col for col in list(df.columns) if df[col].dtype == 'object' if col not in target_col]
    cont_features = [col for col in list(df.columns) if df[col].dtype != 'object' if col not in target_col]
    df[catg_features] = df[catg_features].astype(str)
    df[target_col] = df[target_col].astype(str)
    # impute missing values for continuous & categorical columns
    for col in catg_features:
        df[col].fillna(df[col].mode()[0], inplace=True)
    for col in cont_features:
        df[col].fillna(0, inplace=True)
    for col in catg_features:
        if df[col].dtypes == 'object':
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col])

    df[target_col] = le.fit_transform(df[target_col])   
    X=df.drop(target_col,axis=1)
    y=df[target_col]
    # Feature scaling
    std_scaling = StandardScaler()
# X = std_scaling.fit_transform(X)
    scaled_df_X = std_scaling.fit_transform(X) 
    X_train, X_test, y_train, y_test = train_test_split(scaled_df_X, y, test_size=0.2, random_state=21)
    model = RandomForestClassifier(n_estimators=500, n_jobs=-1, random_state=42)
    model.fit(X_train, y_train)
    features = X.columns
    importances = model.feature_importances_
    indices = np.argsort(importances)
    k=[features[i] for i in indices]
    v=[importances[i] for i in indices]
    res12 = {}
    for key in k:
        for value in v:
            res12[key] = value
            v.remove(value)
            break
    # Printing resultant dictionary
    jsonString = json.dumps(res12, default=str)
    return jsonString

@DataProfile.route('/api/getModelPrediction', methods=['POST']) 
def getModelPrediction():
    content = request.get_json()
    sourcepath= content['sourcepath']
    target_col = content['target_column']
    target_model = content['target_model']
    df = df_from_path(sourcepath)

    # convert the date and time columns to DateTime type
    df = force_datetime_conversion(df)

    catg_features = [col for col in list(df.columns) if df[col].dtype == 'object' if col not in target_col]
    cont_features = [col for col in list(df.columns) if df[col].dtype != 'object' if col not in target_col]

    # impute missing values for continuous & categorical columns
    for col in catg_features:
        df[col].fillna(df[col].mode()[0], inplace=True)
    for col in cont_features:
        df[col].fillna(0, inplace=True)

    # Feature selection
    X = df.drop(target_col, axis=1)
    y = df[target_col]

    counts = collections.Counter(y)
    class_name = list(counts)
    target_encode = {value:key for (key, value) in enumerate(class_name)}
    target_decode = {key:value for (key, value) in enumerate(class_name)}
    y = y.map(target_encode)
    print("Target_map:", target_encode)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=21)
    X_test_raw = X_test.copy()

    # Label encode the categorical columns
    for col in X_train.columns:
        if X_train[col].dtypes == 'object':
            le = LabelEncoder()
            X_train[col] = le.fit_transform(X_train[col])
    for col in X_test.columns:
        if X_test[col].dtypes == 'object':
            le = LabelEncoder()
            X_test[col] = le.fit_transform(X_test[col])

    # Feature scaling
    std_scaling = StandardScaler()
    X_train = std_scaling.fit_transform(X_train)
    X_test = std_scaling.fit_transform(X_test)    
    
    models = {
        'LogisticRegression' : LogisticRegression(),
        'Ridge_Classifier' : RidgeClassifier(),
        'DecisionTree_Classifier' : DecisionTreeClassifier(),
        'SupportVector_Classifier' : SVC(),
        'GaussianNaiveBayes': GaussianNB(),
        'KNeighbors_Classifier' : KNeighborsClassifier(),
        'RandomForest_Classifier' : RandomForestClassifier(),
        'XG_Boost_Classifier' : XGBClassifier(),
        'Stochastic_Gradient_Descent_Classifier' : SGDClassifier(),
        'Bagging_Classifier' :BaggingClassifier(),
        'AdaBoost_Classifier' :AdaBoostClassifier(),
        'GradientBoosting_Classifier' : GradientBoostingClassifier()
        }
    scores_dict = {}
    model = models[target_model]
    model.fit(X_train,y_train)
    y_pred = model.predict(X_test)
    y_pred_df = pd.Series(y_pred)
    y_test = y_test.map(target_decode)
    y_pred_df = y_pred_df.map(target_decode)
    df_result_tmp = pd.concat([X_test_raw, y_test], axis=1).reset_index(drop=True)
    df_result = pd.concat([df_result_tmp, y_pred_df], axis=1)
    df_result.rename(columns={0: 'Prediction'}, inplace=True)
    # print(df_result)
    # resdata = df_result.to_dict()
    # jsonString = json.dumps(resdata, default=str)

    resDf = df_result.where(pd.notnull(df_result), 'None')
    data_dict = resDf.to_dict('index')
    result={}
    result['Preview'] = data_dict
    result['Top'] = Top_three_percentage(df)
    jsonString = json.dumps(result, default=str)
    return jsonString


@DataProfile.route('/api/business_term', methods=['POST']) 
def business_term():  
    uploaded_files = request.files.getlist("file[]")
    sourcePath=""
    for file in uploaded_files:
            filename = secure_filename(file.filename)
            if filename != '':
                    #print(sourceFilename)
                    #print(filename)
                    file_ext = os.path.splitext(filename)[1]
                    file_identifier = os.path.splitext(filename)[0]
                    sqllitepath= 'sqlite:///'+file_identifier+'.db'
                    # sqllitepath= 'sqlite:///'+sourceFilename+'.db'
                    # sourceFilename=sourceFilename+file_ext
                    # #print(file_ext)
                    if file_ext in ['.csv', '.xlsx', '.xls', '.json', '.xml'] :
                        dli=[]
                        # sourcePath=sourceFilename
                        sourcePath = filename
                        file.save(sourcePath)
                        
                        try:
                            fileize= os.path.getsize(sourcePath)
                            filesize = fileize/(1024*1024)
                            if filesize>15:
                                SaveFileAsSQlLite(sourcePath,sqllitepath)                            
                        except:
                           i=1
    sourcepath= sourcePath  
    df = df_from_path_uploadSource(sourcepath)
    selectedColumns = list(df.columns.values)
    resdata = {}
    with open('dCatalogue.json', 'r') as openfile:
        json_object = json.load(openfile)
    data = json_object
    populated = []
    for item in data['datacatalogues']:
        sel_col_low = [x.lower() for x in selectedColumns]
        if item['Column'].lower() in sel_col_low:
            col_idx = selectedColumns.index(df.columns[df.columns.get_loc(item['Column'])])
            confidence_level = 100.0 if item['Column'].lower() == sel_col_low[col_idx] else 80.0
            populated.append({
                "ColumnName": selectedColumns[col_idx],
                "BusinessTerm": item["BusinessTerm"],
                "ConfidenceLevel": confidence_level
            })
    return json.dumps(populated)