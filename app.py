from flask import Flask, request, render_template, send_from_directory
from flask_cors import CORS
#from flaskwebgui import FlaskUI
import pandas as pd
import json
import os
import os.path
from io import StringIO
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_datetime64_any_dtype
from werkzeug.utils import secure_filename
import mimetypes
import secrets
import string
from datetime import date
from pprint import pprint
import datetime
import threading
import multiprocessing as mp
import logging
import time
from itertools import combinations
import numpy as np
from ClassifyColumns import classify_columns
from sendMail import sendMailtoUser
import re
from CorrelationFunctions import compute_associations
from pathlib import Path
import cx_Oracle
import itertools
from functools import reduce
import collections
from MDM import editRulesetmdmglobal,getlistfromMDBglobal,CreateRulesetmdm_global,getprimarykey,getMasterDMview,getMasterGOldenview, configureSource_mdm1,editconfiguredsource,getrules_mdm,getrules_mdmglobal,getlistfromMDB,getlistfromMDB_vendorBased,CreateRulesetmdm,editRulesetmdm,checkSourceNameAvailability_Ndm
from dataLineage import EntryDataLineage ,dataLineageforcolumn       
from Datacatlog import getcatalogue,editDatacatalogue,createDatacatalogue,getcatalogueforcolumns
from dataQuality import checkQuality,checkDataQuality,savequlaitychecktodb
#from reports import reports
## ML related libraries
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

from DataProfile import DataProfile

from DataClean import DataClean
from module_dqm import module_dqm
from common_PC_Function import *
from profile_api import profile_api

import csv
from module_plot import *
### Import for PyMongo
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import dns
print("PyMongo Version:",pymongo.__version__)
dns.resolver.default_resolver=dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers=['8.8.8.8']

app = Flask(__name__, static_folder='static', template_folder='static')

app.register_blueprint(DataProfile)

app.register_blueprint(DataClean)
app.register_blueprint(module_dqm)
app.register_blueprint(profile_api)
#app.register_blueprint(reports)
CORS(app)
app.config['UPLOAD_EXTENSIONS'] = ['.csv', '.xlsx', '.xls', '.json', '.xml']
app.config['MAX_CONTENT_LENGTH'] = 10000* 1024 * 1024

@app.route("/")
def helloWorld():
  return render_template('index.html')


@app.route("/static/")
def helloWorld1():
  return render_template('index.html')





@app.route('/classify_cols',methods=['GET'])
def classify_cols():
    ''' Read a CSV file from a url into a pandas dataframe and then classify_columns into various datatypes'''

    # df = pd.read_excel('Titanic.xls')
    # csv_data = df.to_csv(index=None)
    # df = pd.read_csv(StringIO(csv_data))
    df = pd.read_csv('flights - Copy.csv')
    output = classify_columns(df, verbose=0)
    print(output)
    message = """\
                From: From DQ Analytics <from@fromdomain.com>
                To: To Person <to@todomain.com>
                MIME-Version: 1.0
                Content-type: text/html
                Subject: Notifcation- DQ Analytics
                """
    receiver_email = "givingactuallylive@gmail.com"
    message=message+ """\
        <h1>The file is uploaded and the results are available in
    DashBoard for the source Named
    """
    message= message+ """\
        datetttt </h1>"""

    sendMailtoUser(message,receiver_email)
    '''df= pd.read_csv("flights_new lot of empty cells - DL.csv")
    output = classify_columns(df, verbose=2)
    print(output['cat_vars'])
    jsonString = json.dumps(output, default=str)'''
    jsonString=""
    return jsonString

@app.route("/oracle_connectivitytest")
def TestOracleConnectivity():
    import oracledb
    print ("stating")
    conn = oracledb.connect(user="hr", password="oracle", dsn="localhost:1521/XEPDB1")
    print ("connection established")
    with conn.cursor() as cur:
        cur.execute("SELECT 'Hello World!' FROM dual")
        res = cur.fetchall()
        print(res)
    return "true"

def int_cat_datatype_conv(cols_data_type_dict, df):
  '''Pass list of column names of data type "int_vars" to check and classify as type - cat_vars '''
  int_col_unique_values = {}
  for col in cols_data_type_dict['int_vars']:
    unique_vals = df[col].value_counts().index.to_list()
    unique_vals_value_counts = df[col].value_counts().values
    ## Append unique values of int columns to a dictionary
    int_col_unique_values[col] = unique_vals
    least_value_count = unique_vals_value_counts[-1]
    last_unique_val = unique_vals[-1]
    #print('\nUnique Values of int_var column - {}: {}.\nValue_counts in descending order for unique_values {}: {}'.format(col,sorted(unique_vals),unique_vals,unique_vals_value_counts))
    #print('Column: {}. Unique element: {} has the Least value count: {}'.format(col, last_unique_val,least_value_count))
    if len(int_col_unique_values[col]) < 25 and least_value_count > 9: ## Check for our two conditions
      suggested_data_type = 'cat_vars'
      cols_data_type_dict['int_vars'].remove(col)
      cols_data_type_dict['cat_vars'].append(col)
      #print('Column: {} meets conditions, as it has less than 25 unique values and Least value count for the last element {} is: {}'.format(col, last_unique_val,least_value_count))
      #print('NEW Suggested Data Type for {} column:'.format(col),suggested_data_type)
  return cols_data_type_dict

def force_datetime_conversion(df):
  '''Force convert cols with Date or Time related column names to datetime format'''
  date_ref_list = ["Date","date","Datum","datum"]
  time_ref_list = ["Time",'time']
  date_cols = list(set(date_ref_list).intersection(set(df.columns)))
  time_cols = list(set(time_ref_list).intersection(set(df.columns)))
  if len(date_cols) != 0:
    for col_d in date_cols:
      df[col_d] = pd.to_datetime(df[col_d], dayfirst= True) #.apply(lambda x: x.strftime(r'%d/%m/%Y')) #df[date_name_cols].apply(pd.to_datetime)
  if len(time_cols) != 0:
    for col_t in time_cols:
      df[col_t] = pd.to_datetime(df[col_t]) #.apply(lambda x: x.strftime(r'%H:%M:%S'))
  else:
      pass
  return df

def df_from_path(sourcepath):
    '''Returns a pandas dataframe when given a source path'''
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]

    fileize= os.path.getsize(sourcepath)
    filesize = fileize/(1024*1024)
    if filesize>15:
        df=getchunkforPreview(sourcepath,3000)
    else:
        if(file_ext==".csv"):
            df = pd.read_csv(sourcepath)
        elif(file_ext==".xls"):
            df = pd.read_excel(sourcepath)
            csv_data = df.to_csv(index = False)
            df = pd.read_csv(StringIO(csv_data))
        elif(file_ext==".xlsx"):
            df = pd.read_excel(sourcepath)
            csv_data = df.to_csv(index = False)
            df = pd.read_csv(StringIO(csv_data))
        elif(file_ext==".json"):
            data = json.load(open(sourcepath,"r"))
            print("Type of JSON data:",type(data))
            if type(data) == dict and len(list(data.keys())) != 0:
                msg1 = "JSON file contains the root key(s):- {}.Please reformat JSON file without any root keys.".format(list(data.keys()))
                msg2 = "JSON file should store the tabular data in a list of dictionaries like this [{},{},{}..] WITHOUT any root keys."
                err_msg = {"Error":msg1+msg2}
                return err_msg
            else:
                df =  pd.DataFrame(data)
        elif(file_ext==".xml"):
            try:
                df = pd.read_xml(sourcepath)
            except:
                err_msg = {"Error":"XML file read error. XML file has faulty structure which mismatches the reference example or\
                            XML file may contain column names with whitespaces or special characters other than '_' "}
                return err_msg
    df = force_datetime_conversion(df) ##Force DataType conversion to suitable date/time cols
   
    return df



def df_from_pathwithoutTimeConversion(sourcepath):
    '''Returns a pandas dataframe when given a source path'''
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]

    fileize= os.path.getsize(sourcepath)
    filesize = fileize/(1024*1024)
    if filesize>15:
        df=getchunkforPreview(sourcepath,3000)
    else:
        if(file_ext==".csv"):
            df = pd.read_csv(sourcepath)
        elif(file_ext==".xls"):
            df = pd.read_excel(sourcepath)
            csv_data = df.to_csv(index = False)
            df = pd.read_csv(StringIO(csv_data))
        elif(file_ext==".xlsx"):
            df = pd.read_excel(sourcepath)
            csv_data = df.to_csv(index = False)
            df = pd.read_csv(StringIO(csv_data))
        elif(file_ext==".json"):
            data = json.load(open(sourcepath,"r"))
            print("Type of JSON data:",type(data))
            if type(data) == dict and len(list(data.keys())) != 0:
                msg1 = "JSON file contains the root key(s):- {}.Please reformat JSON file without any root keys.".format(list(data.keys()))
                msg2 = "JSON file should store the tabular data in a list of dictionaries like this [{},{},{}..] WITHOUT any root keys."
                err_msg = {"Error":msg1+msg2}
                return err_msg
            else:
                df =  pd.DataFrame(data)
                csv_data = df.to_csv(index = False)
                df = pd.read_csv(StringIO(csv_data))
        elif(file_ext==".xml"):
            try:
                df = pd.read_xml(sourcepath)
            except:
                err_msg = {"Error":"XML file read error. XML file has faulty structure which mismatches the reference example or\
                            XML file may contain column names with whitespaces or special characters other than '_' "}
                return err_msg
    
   
    return df

#region ProfileClean_SourceUploadEdit

@app.route('/api/GetAllMDMdBSourceList', methods=['POST'])
def GetAllMDMdBSourceList():
    content = request.get_json()   
   
    jsonString=getlistfromMDB(content)
    return jsonString

@app.route('/api/GetAllMDMdBSourceList_v1', methods=['POST'])
def GetAllMDMdBSourceListv1():
    content = request.get_json()   
   
    jsonString=getlistfromMDB_vendorBased(content)
    return jsonString


@app.route('/api/GetAllMDMdBGLobalRuleList', methods=['POST'])
def GetAllMDMdBSourceListglo():
    content = request.get_json()   
   
    jsonString=getlistfromMDBglobal(content)
    return jsonString

@app.route('/getMasterDMview',methods=['POST'])
def getMasterDMview_mdm():
    content = request.get_json()  
    s=content['sourcelogs']
    settings=[]
    if "settings" in content:
        settings=content["settings"]
    res=getMasterDMview(s,settings)#getprimarykey()
    return json.dumps(res)

@app.route('/getMasterGoldenview',methods=['POST'])
def getMastergoldemview_mdm():
    content = request.get_json()  
    s=content['sourcelogs']
    res=getMasterGOldenview(s)#getprimarykey()
    return json.dumps(res)


@app.route('/api/QuerydBSourceList', methods=['POST'])
def QuerydBSourceList():
    '''API to query the profiledb json log file'''
    content = request.get_json()
    query_col = content["query_col"]
    query_val = content["query_val"]
    db = content["db"]
    if db == "profile":
        data = json.load(open("profiledb.json","r"))
    elif db == "clean":
        data = json.load(open("cleandb.json","r"))
    proc_data = []
    for row in data["SourceDetailsList"]:
        proc_data.append(row)
    if len(proc_data) == 0:
        return {"message": "{}DB is empty".format(db)}
    else:
        df = pd.DataFrame(data = proc_data)
        df_result = df[df[query_col] == query_val]
    return df_to_json(df_result.reset_index(drop=True))

def listSourceNamesPC(db):
    if db == "profile":
        with open('profiledb.json', 'r') as openfile:
            json_object = json.load(openfile)
    elif db == "clean":
        with open('cleandb.json', 'r') as openfile:
            json_object = json.load(openfile)
    data = json_object
    IDList=[]
    for obj in data['SourceDetailsList']:
        IDList.append((obj["sourceDataName"]))
    return (IDList)


#endregion ProfileClean_SourceUploadEdit
@app.route('/api/checkSourceNameAvailability', methods=['POST'])
def checkSourceNameAvailability():
    content = request.get_json()
    sourceDataName = content["sourceName"]
    data_dict={}
    listA = listSourceNamesCommon()
    if sourceDataName in listA:
        content['errorMsg']= 'The source name already exists'
        content['errorflag']= 'True'
        content['errorCode']= '101'
        return json.dumps(content, default=str)
    else:
        content['errorMsg']= ''
        content['errorflag']= 'False'
        content['errorCode']= ''
        content['success']= 'Okay'
        return json.dumps(content, default=str)

@app.route('/api/checkSourceNameAvailability_mdm', methods=['POST'])
def checkSourceNameAvailability_mdm():
    content = request.get_json()
    res=checkSourceNameAvailability_Ndm(content)
    return res

def listSourceNamesCommon():
    with open('commondb.json', 'r') as openfile:
        rjson_object = json.load(openfile)

    rdata=rjson_object
    data= rdata['commonSourceList']
    df = pd.DataFrame.from_dict(data)
    IDList=[]
    if not df.empty:
        IDList = df["sourceDataName"].tolist()  
    return (IDList)

@app.route('/api/configureSource_MDM', methods=['POST'])
def configureSource_mdm():
    uploaded_files  = request.files.getlist("file[]")
    content =json.loads(request.form.get('data'))
    jsonString=configureSource_mdm1(uploaded_files,content)
    return jsonString

@app.route('/api/configureSource_MDM', methods=['PUT'])
def editconfiguresource_mdm():
    uploaded_files  = request.files.getlist("file[]")
    reference_files = request.files.getlist("reffile[]")
    print("Reference Files:\n",reference_files, "\nLength of Reference Files:",len(reference_files))
    content =json.loads(request.form.get('data'))
    jsonString = editconfiguredsource(uploaded_files,content)
    return jsonString

@app.route('/api/configureSource', methods=['POST'])
def configureSource():
    selectedColumns=[]
    uploaded_files  = request.files.getlist("file[]")
    reference_files = request.files.getlist("reffile[]")
    print("Reference Files:\n",reference_files, "\nLength of Reference Files:",len(reference_files))
    content =json.loads(request.form.get('data'))
    sourcedetails= content['source']
    referencedetails= content['reference']
    settings= content['settings']
    createdUser= sourcedetails['createdUser']
    data_dict={}
    if 'isotherDBSource' in sourcedetails:
        isotherDBSource= sourcedetails['isotherDBSource'] # YES /NO
        otherDBSourcePath= sourcedetails['otherDBSourcePath']
        
        if isotherDBSource== "YES":
            uploadReason="otherDBsource"
        print(isotherDBSource)
    else:
        isotherDBSource= 'NO'
        otherDBSourcePath= ''
        print(isotherDBSource)
    #source related details
    sourceDataName= sourcedetails['sourceDataName']
    sourceCategory=sourcedetails['sourceCategory']
    listA = listSourceNames()
    if sourceDataName in listA:
        content['errorMsg']= 'The source name already exists'
        content['errorflag']= 'True'
        content['errorCode']= '101'
        return json.dumps(content, default=str)
    else:
        #sourceDataDescription= sourcedetails['sourceDataDescription']
        sourceFilename = sourcedetails['sourceFileName']
        newSourceId='S'+ str(int(getSourceMaxId())+1)
        sourceFilename=newSourceId + sourceFilename
        df = pd.DataFrame()
        path=""
        if isotherDBSource=="YES":
            df= df_from_path(otherDBSourcePath)
            print("df for othr source")
            print(df.head())
            path=otherDBSourcePath
        else:
            for file in uploaded_files:
                filename = secure_filename(file.filename)
                if filename != '':
                    file_ext = os.path.splitext(filename)[1]
                    if file_ext in app.config['UPLOAD_EXTENSIONS']:
                        dli=[]
                        file.save(sourceFilename)
                        try:
                            path=sourceFilename
                            fileize= os.path.getsize(sourceFilename)
                            filesize = fileize/(1024*1024)
                            print(filesize)
                            file=sourceFilename
                            dli=[]
                            if(file_ext==".csv"):
                                try:
                                    print("in csv")
                                    str_dem = find_delimiter(file)
                                    print("delimiter",str_dem)
                                    if(str_dem =="True"):
                                        if filesize>10:                    
                                            chunksize = 50
                                            df= getchunkforPreview(file,chunksize)
                                        else:
                                            df= df_from_path(file)
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

                            # df= df_from_path(path)
                            selectedColumns=df.columns.values.tolist()
                            if any ('Unnamed' in col for col in df.columns.values.tolist()):
                                    print('Missing Header Columns')
                                    error_msg ="Missing Header column"
                                    raise pd.errors.ParserError
                            
                            for k in selectedColumns:
                                if (df[k].nunique() == 0):
                                    error_msg ="The entire column '{}' contains no value".format(k)
                                    raise pd.errors.EmptyDataError
                        except (pd.errors.ParserError, pd.errors.EmptyDataError):
                            content['errorMsg']= 'Data Parsing Failed. {}'.format(error_msg)
                            content['errorflag']= 'True'
                            content['errorCode']= '105'
                            return json.dumps(content, default=str)
            
        if not df.empty:
                    output = classify_columns(df, verbose=0)
                    output = int_cat_datatype_conv(output, df)
                    catFeat = output['cat_vars'] + output['string_bool_vars']
                    print("Cat. Features (before):", catFeat)
                    if not catFeat:
                        df_meta = pd.DataFrame(df.dtypes).reset_index()
                        df_meta.columns = ["column_name", "dtype"]
                        df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
                        df_meta_inferred.columns = ["column_name", "inferred_dtype"]
                        df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
                        print("df_meta:",df_meta)
                        catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
                    print("Cat. Features (after):", catFeat)
                    df=df.head()
                    resDf = df.where(pd.notnull(df), 'None')
                    data_dict = resDf.to_dict('index')
                    dli= list(df.columns.values)
                    selectedColumns=dli
                    sourcedetails['templateSourcePath']= path
                    sourcedetails['templateSourcePath']= path
                    sourcedetails['availableColumns'] = dli
                    sourcedetails['sqlLitePath']='sqlite:///'+newSourceId+sourceDataName+'.db'
                    try:
                        fileize= os.path.getsize(path)
                        filesize = fileize/(1024*1024)
                        if filesize>15:
                            SaveFileAsSQlLite(path,'sqlite:///'+newSourceId+sourceDataName+'.db')
                            sourcedetails['IsSqlLiteSource']='Yes'
                        else:
                            sourcedetails['IsSqlLiteSource']='No'
                    except:
                        sourcedetails['IsSqlLiteSource']='No'
                    df_meta = pd.DataFrame(df.dtypes).reset_index()
                    df_meta.columns = ["column_name", "dtype"]
                    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
                    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
                    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
                    sourcedetails['categorialColumns'] = catFeat
                    if settings["multiSourceOptions"]:
                        if settings["multiSourceColumn"]!="":
                            if settings["multiSourceColumn"] not in catFeat:
                                catFeat.append(settings["multiSourceColumn"])
                                sourcedetails['categorialColumns'] = catFeat
                    content['source']=  sourcedetails
                    content['sourcePreview']=data_dict
        #reference related details
    N = 7 ### Max Nr. of files to be used for reference - arbitrary set
    i=1
    if content["ref_data_type"] == "User_LocalData" or content["ref_data_type"] == "":
        for file in reference_files:
            strRef=newSourceId+'-Ref' +str(i)+'-'
            refId='Ref'+str(i)
            print("Local User data Ref ID:", refId)
            filename = secure_filename(file.filename)
            if filename != '':
                file_ext = os.path.splitext(filename)[1]
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    dli=[]
                    res = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(N))
                    path= strRef + str(res)+file_ext
                    file.save(path)
                    df = df_from_path(path)
                    print("User Local Ref Data Preview:\n",df.head())
                    dli= list(df.columns.values)
                    dli2=[]
                    dli2 = [strRef + s for s in dli]
                    referencedetails[i-1]['ref_data_type'] = "User_LocalData"
                    referencedetails[i-1]['referenceId'] = refId
                    referencedetails[i-1]['availableRefColumns'] = dli2
                    referencedetails[i-1]['referencePath']     =  path
                    i=i+1
    elif content["ref_data_type"] == "MongoDB_RefData":
        strRef=newSourceId+'-Ref' +str(i)+'-'
        refId='Ref'+str(i)
        print("MongoDB data Ref ID:", refId)
        path = referencedetails[i-1]['referencePath']
        referenceDataName= referencedetails[i-1]['referenceDataName']
        df = get_df_RefSQLlite(referenceDataName,referenceDataName)
        print("MongoDb Ref Data Preview:\n",df.head())
        dli= list(df.columns.values)
        dli2=[]
        dli2 = [strRef + s for s in dli]
        referencedetails[i-1]['ref_data_type'] = "MongoDB_RefData"
        referencedetails[i-1]['referenceId'] = refId
        referencedetails[i-1]['availableRefColumns'] = dli2
        referencedetails[i-1]['referencePath']     =  path
        i=i+1
    data = json.load(open("db.json","r"))
    AnalysisList=  data['Analysis']
    del content["ref_data_type"]
    content['sourceId']= newSourceId
    content['rules']=[]
    AnalysisList.append(content)
    data['Analysis'] = AnalysisList
    json.dump(data, open("db.json","w"), default=str)
    newsourcedetails= sourcedetails
    newsourcedetails['sourceId']= ''
    newsourcedetails['originalsourceId']= newSourceId
    newsourcedetails['sourceDataName']= sourceDataName
    newsourcedetails['sourceCategory']= sourceCategory
    newsourcedetails["uploadDate"]= newSourceId
    newsourcedetails['rules']=[]
    newsourcedetails["sourcePath"]= sourcedetails['templateSourcePath']
    newsourcedetails["isFromDQM"]= 'YES'
    newsourcedetails["department"]=settings["department"]
    if isotherDBSource!="YES":
        addCleaningSourceFromProfile("dqm",newsourcedetails)
        addProfileSourceFromcleaning("dqm",newsourcedetails)
    updatecommondb("dqm",newsourcedetails)
    content['errorMsg']= ''
    content['errorflag']= 'False'
    messagel = "File Received in DQM"
    EntryDataLineage(sourcedetails['templateSourcePath'],newSourceId,  sourceDataName,newSourceId, str(datetime.now()),"DQM",messagel,selectedColumns)
    EntryDataLineage(sourcedetails['templateSourcePath'],newSourceId,  sourceDataName,newSourceId, str(datetime.now()),"DQM",'File storage',selectedColumns)
     
    return json.dumps(content, default=str)


def GetAEntityDB(sourceId):
    analysisList={}
    data={}
    with open('db.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['Analysis']:
        if obj["sourceId"]==sourceId:
            analysisList=obj
            break
    data['Analysis'] = analysisList
    jsonString = json.dumps(data, default=str)
    return jsonString

@app.route('/api/configureSource', methods=['PUT'])
def EditconfigureSource():
    selectedColumns=[]
    uploaded_files = request.files.getlist("file[]")
    reference_files = request.files.getlist("reffile[]")
    content =json.loads(request.form.get('data'))
    sourcedetails= content['source']
    sourceId=content['sourceId']
    referencedetails= content['reference']
    settings= content['settings']
    data_dict={}
    datafromdb=  json.loads(removeAEntityDB(sourceId))
    allDBList= datafromdb['Analysis']
    tobeedited= json.loads(GetAEntityDB(sourceId))
    datatobeedited= tobeedited['Analysis']
    rules=datatobeedited['rules']
    #source related details
    sourceDataName= sourcedetails['sourceDataName']
    listA= listSourceNames()
    #sourceDataDescription= sourcedetails['sourceDataDescription']
    sourceFilename = sourcedetails['sourceFileName']
    newSourceId=sourceId
    datasource= datatobeedited['source']
    datareference= datatobeedited['reference']
    isqllitesource= "No"
    print(datasource)
    sourceFilename= newSourceId+ ""+ sourceFilename
    j=1
    if 'isotherDBSource' in sourcedetails:
        isotherDBSource= sourcedetails['isotherDBSource'] # YES /NO
        otherDBSourcePath= sourcedetails['otherDBSourcePath']
        
        if isotherDBSource== "YES":
            uploadReason="otherDBsource"
        print(isotherDBSource)
    else:
        isotherDBSource= 'NO'
        otherDBSourcePath= ''
        print(isotherDBSource)
    for file in uploaded_files:
        filename = secure_filename(file.filename)
        print(filename)
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                j=j+1
                dli=[]
                file.save(sourceFilename)
                path=sourceFilename
                
                fileize= os.path.getsize(sourceFilename)
                filesize = fileize/(1024*1024)
                print(filesize)
                file=sourceFilename
                df= df_from_path(path)
                output = classify_columns(df, verbose=2)
                output = int_cat_datatype_conv(output, df)
                catFeat = output['cat_vars'] + output['string_bool_vars']
                df=df.head()
                resDf = df.where(pd.notnull(df), 'None')
                data_dict = resDf.to_dict('index')
                dli= list(df.columns.values)
                sourcedetails['templateSourcePath']= path
                sourcedetails['availableColumns'] = dli
                sourcedetails['sqlLitePath']='sqlite:///'+newSourceId+ sourceDataName+'.db'
                try:
                        fileize= os.path.getsize(path)
                        filesize = fileize/(1024*1024)
                        if filesize>15:
                            SaveFileAsSQlLite(path,'sqlite:///'+newSourceId+ sourceDataName+'.db')
                            sourcedetails['IsSqlLiteSource']='Yes'
                        else:
                            sourcedetails['IsSqlLiteSource']='No'
                except:
                        sourcedetails['IsSqlLiteSource']='No'
                df_meta = pd.DataFrame(df.dtypes).reset_index()
                df_meta.columns = ["column_name", "dtype"]
                df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
                df_meta_inferred.columns = ["column_name", "inferred_dtype"]
                df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
                #catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
                sourcedetails['categorialColumns'] = catFeat
                content['sourcePreview']=data_dict

    if(j==1) :
        sourcedetails['templateSourcePath']= datasource['templateSourcePath']
        sourcedetails['availableColumns'] = datasource['availableColumns']
        sourcedetails['categorialColumns'] = datasource['categorialColumns']
        sourcedetails['IsSqlLiteSource']= datasource['IsSqlLiteSource']
    content['source']=  sourcedetails

    #reference related details
    N = 7
    i=1
    if content["ref_data_type"] == "User_LocalData" or content["ref_data_type"] == "":
        for file in reference_files:
            strRef=newSourceId+'-Ref' +str(i)+'-'
            refId='Ref'+str(i)
            print("Local User data Ref ID:", refId)
            filename = secure_filename(file.filename)
            if filename != '':
                file_ext = os.path.splitext(filename)[1]
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    dli=[]
                    res = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(N))
                    path= strRef + str(res)+file_ext
                    file.save(path)
                    df = df_from_path(path)
                    print("User Local Ref Data Preview:\n",df.head())
                    dli= list(df.columns.values)
                    selectedColumns=dli
                    dli2=[]
                    dli2 = [strRef + s for s in dli]
                    referencedetails[i-1]['ref_data_type'] = "User_LocalData"
                    referencedetails[i-1]['referenceId'] = refId
                    referencedetails[i-1]['availableRefColumns'] = dli2
                    referencedetails[i-1]['referencePath']     =  path
                    i=i+1
    elif content["ref_data_type"] == "MongoDB_RefData":
        strRef=newSourceId+'-Ref' +str(i)+'-'
        refId='Ref'+str(i)
        print("MongoDB data Ref ID:", refId)
        path = referencedetails[i-1]['referencePath']
        referenceDataName= referencedetails[i-1]['referenceDataName']
        df = get_df_RefSQLlite(referenceDataName,referenceDataName)
        print("MongoDb Ref Data Preview:\n",df.head())
        dli= list(df.columns.values)
        dli2=[]
        dli2 = [strRef + s for s in dli]
        referencedetails[i-1]['ref_data_type'] = "MongoDB_RefData"
        referencedetails[i-1]['referenceId'] = refId
        referencedetails[i-1]['availableRefColumns'] = dli2
        referencedetails[i-1]['referencePath']     =  path
        i=i+1

    if(i==1) :
        k=1
        for itlm in referencedetails:
            referencedetails[k-1]['referencePath']= datareference[k-1]['referencePath']
            referencedetails[k-1]['availableRefColumns'] = datareference[k-1]['availableRefColumns']
            k=k+1
    content['reference']=referencedetails
    content['rules']=rules
    del content["ref_data_type"]
    allDBList.append(content)

    data={}
    data['Analysis'] = allDBList
    json.dump(data, open("db.json","w"), default=str)
    
    editcommondb(sourceId,sourcedetails=sourcedetails)
    newsourcedetails= sourcedetails
    sourceCategory=sourcedetails['sourceCategory']
    newsourcedetails['rules']=[]
    newsourcedetails['originalsourceId']= sourceId
    newsourcedetails['sourceDataName']= sourceDataName
    newsourcedetails['sourceCategory']= sourceCategory
    newsourcedetails["uploadDate"]= newSourceId
    newsourcedetails['rules']=[]
    newsourcedetails["sourcePath"]= sourcedetails['templateSourcePath']
    newsourcedetails["isFromDQM"]= 'YES'
    newsourcedetails["department"]=settings["department"]
    newsourcedetails["department"]=settings["department"]
    if isotherDBSource!="YES":
        editProfileSourceFromcleaning("clean",sourceId,sourcedetails=newsourcedetails)
        editCleaningSourceFromProfile("profile",sourceId,sourcedetails=newsourcedetails)
    messagel = "File Received in DQM"
    EntryDataLineage(sourcedetails['templateSourcePath'],sourceId,  sourceDataName,sourceId, str(datetime.now()),"DQM",messagel,selectedColumns)
        
    return json.dumps(content, default=str)



@app.route('/getSourceMaxId', methods=['GET'])
def getSourceMaxId():
        with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        dictionary ={}
        IDList=[]
        IDList.append('0')
        for obj in data['Analysis']:
                    IDList.append((obj["sourceId"])[1:])
        dictionary["IdsList"] = IDList
        print(str(IDList))
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))

def removeAEntityDB(sourceId):
        analysisList=[]
        data={}
        with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        for obj in data['Analysis']:
            if obj["sourceId"]!=sourceId:
                analysisList.append(obj)
        data['Analysis'] = analysisList
        jsonString = json.dumps(data, default=str)
        return jsonString

@app.route('/listSourceNames', methods=['GET'])
def listSourceNames():
        with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        IDList=[]
        IDList.append('')
        for obj in data['Analysis']:
            sourceobj= obj['source']
            IDList.append((sourceobj["sourceDataName"]))
        return (IDList)

@app.route('/api/CreateRuleSet', methods=['POST'])
def CreateRuleset():
    data={}
    content= request.get_json()
    sourceId=content['sourceId']
    newRules = content['ruleset']
    data=  json.loads(removeAEntityDB(sourceId))

    AnalysisList= data['Analysis']

    rulesObject={}
    datarules= json.loads(GetAEntityDB(sourceId))
    datarulesList= datarules['Analysis']
    existingrulesList=[]
    existingrulesList=datarulesList['rules']
    rulesObject['rulesetId']=sourceId + 'R' + str(int(getRMaxId(sourceId))+1)
    rulesObject['selectedColumns'] = content['selectedColumns']
    rulesObject['refSelectedColumns'] = content['refSelectedColumns']
    rulesObject['startDate'] = content['startDate']
    rulesObject['endDate'] = content['endDate']
    #rulesObject['Referencepath'] = content['Referencepath']
    rulesObject['rulesetName']= content['rulesetName']
    rulesObject['rulesfor2cols']= content['rulesfor2cols']
    rulesObject['rulesfor3cols']= content['rulesfor3cols']
    rulesObject['rulesformulticols']= content['rulesformulticols']
    rulesObject['ruleset']=newRules
    content['rulesetId']=rulesObject['rulesetId']
    existingrulesList.append(rulesObject)

    datarulesList['rules'] =existingrulesList

    AnalysisList.append(datarulesList)
    data['Analysis'] = AnalysisList
    json.dump(data, open("db.json","w"), default=str)
    return json.dumps(content, default=str)

@app.route('/api/CreateRuleSet', methods=['PUT'])
def editRuleset():
    data={}
    content= request.get_json()
    sourceId=content['sourceId']
    newRules = content['ruleset']
    rulesetId= content['rulesetId']

    data=  json.loads(removeAEntityDB(sourceId))
    AnalysisList= data['Analysis']

    rulesObject={}
    datarules= json.loads(GetAEntityDB(sourceId))
    datarulesList= datarules['Analysis']
    existingrulesListedited=[]
    for obj1 in datarulesList['rules']:
        if obj1["rulesetId"]!=rulesetId:
            existingrulesListedited.append(obj1)
    rulesObject['selectedColumns'] = content['selectedColumns']
    rulesObject['refSelectedColumns'] = content['refSelectedColumns']
    #rulesObject['Referencepath'] = content['Referencepath']
    rulesObject['startDate'] = content['startDate']
    rulesObject['endDate'] = content['endDate']
    rulesObject['rulesetName']= content['rulesetName']
    rulesObject['rulesfor2cols']= content['rulesfor2cols']
    rulesObject['rulesfor3cols']= content['rulesfor3cols']
    rulesObject['rulesformulticols']= content['rulesformulticols']
    rulesObject['ruleset']=newRules
    rulesObject['rulesetId']=rulesetId
    existingrulesListedited.append(rulesObject)

    datarulesList['rules'] =existingrulesListedited

    AnalysisList.append(datarulesList)
    data['Analysis'] = AnalysisList
    json.dump(data, open("db.json","w"), default=str)
    return json.dumps(content, default=str)



@app.route('/api/CreateRuleSet_mdm', methods=['POST'])
def CreateRuleset_mdm():
    data={}
    content= request.get_json()
    results=CreateRulesetmdm(content)
    return results

@app.route('/api/CreateRuleSet_mdm', methods=['PUT'])
def editRuleset_mdm():
    data={}
    content= request.get_json()
    
    results=editRulesetmdm(content)
    return results



@app.route('/api/CreateRuleSet_Globalmdm', methods=['POST'])
def CreateRuleset_Globalmdm():
    data={}
    content= request.get_json()
    results=CreateRulesetmdm_global(content)
    return results

@app.route('/api/CreateRuleSet_Globalmdm', methods=['PUT'])
def editRuleset_Gmdm():
    data={}
    content= request.get_json()
    
    results=editRulesetmdmglobal(content)
    return results


@app.route('/api/listSource', methods=['GET'])
def listSource():
        with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        jsonString = json.dumps(data['Analysis'])
        return (jsonString)

@app.route('/api/ruleswithoutthread', methods=['POST']) #GET requests will be blocked
def rulesWithRef():
    content = request.get_json()
    sourcepath= content['sourcepath']
    connectiondetails={}
    typeofcon=""
    if "type" in content:
        if content["type"]== "oracle":
            connContent=content["connectionDetails"]
            connectiondetails= content["connectionDetails"]
            typeofcon = "oracle"
            host=connContent['host']
            dbName = connContent['dbName']
            userName = connContent['username']
            port = connContent['port']
            password = connContent['password']
            table = connContent['sourceTableName']
            df = getdfFromTable(host,dbName,port,userName,password,table)
    else:
        df= df_from_pathwithoutTimeConversion(sourcepath)
       
    print(df)
    rules=[]
    selectedColumns= content['selectedColumns']
    df_meta = pd.DataFrame(df.dtypes).reset_index()
    df_meta.columns = ["column_name", "dtype"]
    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")

    conFeat = df_meta[df_meta.dtype != "object"].column_name.tolist()
    catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
    print(conFeat)
    print(catFeat)
    if(len(conFeat)>0):
        #NumericCorrMatrix=GetCorrelationMatrix(sourcepath, 'numeric','pearson')
        if typeofcon == "oracle":
            NumericCorrMatrix=GetCorrelationMatrix_v1(df, 'numeric','pearson',"oracle",connectiondetails )
        else:
            NumericCorrMatrix=GetCorrelationMatrix(df, 'numeric','pearson')
    if(len(catFeat)>0):
        #CateoricalCorrMatrix=GetCorrelationMatrix(sourcepath, 'categorical' ,'theils_u')
        if typeofcon == "oracle":
            CateoricalCorrMatrix=GetCorrelationMatrix_v1(df, 'categorical' ,'theils_u',"oracle",connectiondetails )
        else:            
            CateoricalCorrMatrix=GetCorrelationMatrix(df, 'categorical' ,'theils_u')
            
    refColumns= content['refSelectedColumns']
    for k in selectedColumns:
        statisticAnal= []
        Dict = {}
        if k in conFeat:
            df_describe_continuous = processContinuous([k], df)
            Dict['statistics'] =df_describe_continuous
            ResMatrix=sorted_corr_matrix_per_col(NumericCorrMatrix,k)
            Dict['correlationSummary']=ResMatrix
            statisticAnal=df_describe_continuous
        if k in catFeat:
            df_describe_categorical = processCategorical([k], df)
            Dict['statistics'] =df_describe_categorical
            
            try:
                ResMatrix=sorted_corr_matrix_per_col(CateoricalCorrMatrix,k)
                Dict['correlationSummary']=ResMatrix    
            except:
                Dict['correlationSummary']={}
            statisticAnal=df_describe_categorical

        corrMatrix={}
        RulesDict = {}
        rules_list=[]
        if is_string_dtype(df[k]):
            RulesDict['rule'] ='DataType'
            isAlphacount = df[k].str.isalpha().sum()
            isAlphaNumcount= df[k].str.isalnum().sum() - isAlphacount
            print(isAlphacount)
            print(isAlphaNumcount)
            '''if(isAlphacount>isAlphaNumcount)  :
                RulesDict['value'] =  'Text'
            else:
                RulesDict['value'] =  'Alphanumeric
            '''
            for aval in statisticAnal:
                RulesDict['value']=aval['suggested_dtype']
                if (aval['suggested_dtype']=="Alphabetic"):
                    RulesDict['value'] =  'Text'


            RulesDict['dimension'] =  'Validity'
            RulesDict['operator']  = 'Shouldbe'
            RulesDict['format']  = ''
            rules_list.append(RulesDict)
            if (aval['suggested_dtype']!="Numeric"):
                minLength= df[k].str.len().min()
                maxLength=df[k].str.len().max()
                if (minLength)== (maxLength):
                    RulesDict1 = {}
                    RulesDict1['rule'] ='Length'
                    RulesDict1['value'] = str(maxLength)
                    RulesDict1['dimension'] =  'Validity'
                    RulesDict1['operator']  = 'equalto'
                    RulesDict1['format']  = ''
                    rules_list.append(RulesDict1)
                else:
                    MinRulesDict1 = {}
                    MinRulesDict1['rule'] ='MaxLength'
                    MinRulesDict1['value'] = str(maxLength)
                    MinRulesDict1['dimension'] =  'Validity'
                    MinRulesDict1['operator']  = 'equalto'
                    MinRulesDict1['format']  = ''
                    rules_list.append(MinRulesDict1)

                    MaxRulesDict1 = {}
                    MaxRulesDict1['rule'] ='MinLength'
                    MaxRulesDict1['value'] = str(minLength)
                    MaxRulesDict1['dimension'] =  'Validity'
                    MaxRulesDict1['operator']  = 'equalto'
                    MaxRulesDict1['format']  = ''
                    rules_list.append(MaxRulesDict1)

        if is_numeric_dtype(df[k]):
            RulesDict['rule'] ='DataType'
            RulesDict['value'] =  'Numeric'
            for aval in statisticAnal:
                RulesDict['value']=aval['suggested_dtype']
            RulesDict['dimension'] =  'Validity'
            RulesDict['operator']  = 'Shouldbe'
            RulesDict['format']  = ''
            rules_list.append(RulesDict)

            df['counts'] = df[k].astype(str).apply(count_digits)
            minLength= df['counts'].min()
            maxLength=df['counts'].max()
            if (minLength)== (maxLength):
                    RulesDict1 = {}
                    RulesDict1['rule'] ='Length'
                    RulesDict1['value'] = str(maxLength)
                    RulesDict1['dimension'] =  'Validity'
                    RulesDict1['operator']  = 'equalto'
                    RulesDict1['format']  = ''
                    rules_list.append(RulesDict1)
            else:
                    MinRulesDict1 = {}
                    MinRulesDict1['rule'] ='MaxLength'
                    MinRulesDict1['value'] = str(maxLength)
                    MinRulesDict1['dimension'] =  'Validity'
                    MinRulesDict1['operator']  = 'equalto'
                    MinRulesDict1['format']  = ''
                    rules_list.append(MinRulesDict1)

                    MaxRulesDict1 = {}
                    MaxRulesDict1['rule'] ='MinLength'
                    MaxRulesDict1['value'] = str(minLength)
                    MaxRulesDict1['dimension'] =  'Validity'
                    MaxRulesDict1['operator']  = 'equalto'
                    MaxRulesDict1['format']  = ''
                    rules_list.append(MaxRulesDict1)
            print(df[k].nunique())
            if df[k].nunique()>10:
                print('call correlaioships')
                valueofFormula= getCorelationrelationships(df,k)
                corrMatrix= valueofFormula['corrMatrix']
                RulesDictf={}
                if len(valueofFormula['value']) != 0:
                    RulesDictf['rule'] ='Formula'
                    RulesDictf['value'] =  valueofFormula['value']
                    RulesDictf['dimension'] =  'Accuracy'
                    RulesDictf['operator']  = 'equalto'
                    RulesDictf['type']='SIMPLE'
                    RulesDictf['format']  = ''
                    rules_list.append(RulesDictf)
      
            
        if any(k in s for s in refColumns):
            RulesDict2 = {}
            RulesDict2['rule'] ='ReferenceCDE'
            matchers = []
            matchers.append(k)
            matching = [i for i in refColumns if k in i]
            RulesDict2['value'] =  (list(matching))[0]
            RulesDict2['dimension'] =  'Integrity'
            RulesDict2['operator']  = 'Shouldbe'
            RulesDict2['format']  = ''
            rules_list.append(RulesDict2)
            print(str(matching))

        if (df[k].nunique()!=0):
            Dict['column'] =k
            Dict['rules'] = rules_list
            rules.append(Dict)
        else:
            Dict['column'] =k
            Dict['rules'] = rules_list
            Dict['statistics']=[]
            rules.append(Dict)
    json_data = json.dumps(rules, default=str)
    return json_data



@app.route('/getRMaxId', methods=['GET'])
def getRMaxId(sourceId):
        with open('db.json', 'r') as openfile:
            json_object = json.load(openfile)
        data = json_object
        IDList=[]
        IDList.append('0')
        for obj in data['Analysis']:
                if obj["sourceId"]==sourceId:
                   for obj1 in obj["rules"]:
                        IDList.append(obj1["rulesetId"][(len(sourceId)+1):] )
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))


def processContinuous(conFeat, data):
    conHead = ['Count', 'Miss %', 'Card.', 'Min', '1st Qrt.', 'Mean', 'Median', '3rd Qrt', 'Max', 'Std. Dev.',
               'Outliers']
    
    conOutDF = pd.DataFrame(index=conFeat, columns=conHead)
    conOutDF.index.name = 'feature_name'
    columns = data[conFeat]
    
    
    # COUNT
    count = columns.count()
    print(count)
    conOutDF[conHead[0]] = count

    # MISS % - no continuous features have missing data
    percents = [''] * len(conFeat)
    outliers = [''] * len(conFeat)
    for col in columns:
        percents[conFeat.index(col)] = round(columns[col].isnull().sum() / len(columns[col]) * 100, 2)
        anomaly_cut_off = columns[col].std() * 3
        mean = columns[col].mean()
        outliers[conFeat.index(col)] = len(columns[columns[col] < (mean - anomaly_cut_off)]) \
                                       + len(columns[columns[col] > (mean + anomaly_cut_off)])

    conOutDF[conHead[1]] = percents

    # CARDINALITY
    conOutDF[conHead[2]] = columns.nunique()

    # MINIMUM
    conOutDF[conHead[3]] = columns.min()

    # 1ST QUARTILE
    conOutDF[conHead[4]] = columns.quantile(0.25)

    # MEAN
    conOutDF[conHead[5]] = round(columns.mean(), 2)

    # MEDIAN
    conOutDF[conHead[6]] = columns.median()

    # 3rd QUARTILE
    conOutDF[conHead[7]] = columns.quantile(0.75)

    # MAX
    conOutDF[conHead[8]] = columns.max()
    conOutDF['Data Type']='Numeric'
    conOutDF['suggested_dtype'] = 'Numeric'
    # STANDARD DEVIATION
    
    columnsnew=columns.copy()
    columnsnew=columnsnew.fillna(0)
    conOutDF[conHead[9]] = round(columnsnew.std(), 2)
   
    # IQR
    conOutDF[conHead[10]] = outliers

    return  conOutDF.to_dict('records')#.to_json(orient='records')[1:-1].replace('},{', '} {')

def processCategorical(catFeat, data):
    catHead = ['Count', 'Miss #', 'Miss %', 'Card.', 'Len Min', 'Len Max', 'Data Type',
               'Alphabetic', 'Numeric', 'Alphanumeric']

    catOutDF = pd.DataFrame(index=catFeat, columns=catHead)
    catOutDFMeta = None
    catOutDFPattern = None
    catOutDF.index.name = 'feature_name'
    columns = data[catFeat]
    total_rows = len(data)
    # COUNT
    count = columns.count()
    catOutDF[catHead[0]] = count

    # CARDINALITY
    catOutDF[catHead[3]] = columns.nunique()

    # preparing arrays for storing data
    amt = len(catFeat)
    missNumbers = [''] * amt
    missPercents = [''] * amt

    lenMin = [''] * amt
    lenMax = [''] * amt
    dataType = [''] * amt
    dataType2 = [''] * amt
    isAlpha = [''] * amt
    isNum = [''] * amt
    isAlNum = [''] * amt
    isSpace = [''] * amt
    for col in columns:
        values = columns[col].value_counts()
        col_meta = pd.DataFrame(values).reset_index()
        index = catFeat.index(col)

        # MISS %
        missNumbers[index] = columns[col].isnull().sum()
        percent = (missNumbers[index] / total_rows) * 100
        missPercents[index] = round(percent, 2)

        # MODES
        #mode = values.index[0]
        #mode2 = values.index[1]
        #modes[index] = mode
        #modes2[index] = mode2

        # MODE FREQ
        #modeCount = values.loc[mode]
        #modeCount2 = values.loc[mode2]
        #modeFreqs[index] = modeCount
        #modeFreqs2[index] = modeCount2

        # MODE %
        miss = missPercents[index]
        #modePer = (modeCount / (count[index] * ((100 - miss) / 100))) * 100
        #modePercents[index] = round(modePer, 2)

        #modePer2 = (modeCount2 / (count[index] * ((100 - miss) / 100))) * 100
        #modePercents2[index] = round(modePer2, 2)

        # VALUE LENGTH & TYPE
        col_meta = pd.DataFrame(values).reset_index().rename(columns={'index': 'value', col: 'count'})
        col_meta.insert(0, "feature_name", col)
        col_meta['length'] = col_meta.value.apply(lambda x: len(str(x)))
        col_meta['type'] = col_meta.value.apply(lambda x: type(x))
        col_meta['isAlpha'] = col_meta.value.str.isalpha()
        col_meta['isNum'] = col_meta.value.str.isnumeric()
        col_meta['isAlNum'] = col_meta.value.str.isalnum()
        if index == 0:
            catOutDFMeta = col_meta
        else:
            catOutDFMeta = pd.concat([catOutDFMeta, col_meta], ignore_index=True)
        lenMin[index] = col_meta['length'].min()
        lenMax[index] = col_meta['length'].max()
        dataType[index] = col_meta['type'].value_counts().index[0]
        try:
            dataType2[index] = col_meta['type'].value_counts().index[1]
        except Exception as e:
            dataType2[index] = None

        isAlpha[index] = columns[col].str.isalpha().sum()
        isNum[index] = columns[col].str.isnumeric().sum()
        isAlNum[index] = columns[col].str.isalnum().sum() - isAlpha[index]

    catOutDF[catHead[1]] = missNumbers
    catOutDF[catHead[2]] = missPercents
    catOutDF[catHead[4]] = lenMin
    catOutDF[catHead[5]] = lenMax
    catOutDF[catHead[6]] = "object"
    catOutDF[catHead[7]] = isAlpha
    catOutDF[catHead[8]] = isNum
    catOutDF[catHead[9]] = isAlNum
    catOutDF['suggested_dtype'] = catOutDF.filter(items=['Alphabetic', 'Numeric', 'Alphanumeric'], axis=1) \
        .idxmax(axis=1)

    return  catOutDF.to_dict('records')#.to_json(orient='records')[1:-1].replace('},{', '} {')

# 06/06/2022- Thiru #78 (New Proc to handle the date columns)
def processDatetime(dateFeat, data):
    dateHead = ['Count', 'Miss %', 'Card.', 'Min', '1st Qrt.', 'Mean', 'Median', '3rd Qrt', 'Max', 'Std. Dev.',
               'Outliers']
    
    dateOutDF = pd.DataFrame(index=dateFeat, columns=dateHead)
    dateOutDF.index.name = 'feature_name'
    columns = data[dateFeat]
    
    
    # COUNT
    count = columns.count()
    print(count)
    dateOutDF[dateHead[0]] = count

    # MISS % - no continuous features have missing data
    percents = [''] * len(dateFeat)
    outliers = [''] * len(dateFeat)
    for col in columns:
        percents[dateFeat.index(col)] = round(columns[col].isnull().sum() / len(columns[col]) * 100, 2)
        anomaly_cut_off = columns[col].std() * 3
        mean = columns[col].mean()
        outliers[dateFeat.index(col)] = len(columns[columns[col] < (mean - anomaly_cut_off)]) \
                                       + len(columns[columns[col] > (mean + anomaly_cut_off)])
        min_val = data[col].min().strftime('%d-%m-%Y')
        max_val = data[col].max().strftime('%d-%m-%Y')

    dateOutDF[dateHead[1]] = percents

    # CARDINALITY
    dateOutDF[dateHead[2]] = columns.nunique()

    # MINIMUM
    dateOutDF[dateHead[3]] = min_val

    # 1ST QUARTILE
    dateOutDF[dateHead[4]] = 0

    # MEAN
    dateOutDF[dateHead[5]] = 0

    # MEDIAN
    dateOutDF[dateHead[6]] = 0

    # 3rd QUARTILE
    dateOutDF[dateHead[7]] = 0

    # MAX
    dateOutDF[dateHead[8]] = max_val
    dateOutDF['Data Type']='Datetime'
    dateOutDF['suggested_dtype'] = 'Datetime'
    
    # STANDARD DEVIATION
    dateOutDF[dateHead[9]] = 0
    
    # IQR
    dateOutDF[dateHead[10]] = 0

    return  dateOutDF.to_dict('records')


@app.route('/api/getAllAnalysis', methods=['GET'])
def getAllAnalysis():
    analysisList=[]
    data={}

    with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)

    data = json_object
    for obj in data['Analysis']:
        tempDict={}
        tempDict['sourceId']= obj['sourceId']
        tempDict['source']= obj['source']
        sourceData= obj['source']
        tempDict['reference']= obj['reference']
        temprules=[]
        for obj1 in obj["rules"]:
            temprulesDict={}
            temprulesDict['sourceId']= obj['sourceId']
            temprulesDict["rulesetId"]=obj1["rulesetId"]
            temprulesDict["selectedColumns"]=    obj1["selectedColumns"]
            #temprulesDict["Referencepath"]=    obj1["Referencepath"]
            temprulesDict["refSelectedColumns"]=    obj1["refSelectedColumns"]
            temprulesDict['startDate'] = obj1['startDate']
            temprulesDict['endDate'] = obj1['endDate']
            temprulesDict["ruleset"]= obj1["ruleset"]
            temprulesDict["rulesetName"]= obj1["rulesetName"]
            temprulesDict["columns"]= sourceData["availableColumns"]
            startDate_obj =  datetime.strptime(obj1['startDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
            endDate_obj = datetime.strptime(obj1['endDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

            if startDate_obj.date() <= date.today() <= endDate_obj.date():
                temprulesDict["Rulestatus"]= "Active"
            elif startDate_obj.date() >= date.today():
                temprulesDict["Rulestatus"]= "Inactive"
            else:
                temprulesDict["Rulestatus"]= 'Expired'

            temprules.append(temprulesDict)
        tempDict['rules']=temprules
        analysisList.append(tempDict)
    data['Analysis'] = analysisList
    jsonString = json.dumps(data, default=str)
    return jsonString



@app.route('/api/DelayAnalysis', methods=['POST']) #GET requests will be blocked
def DelayAnalysis():

    content = request.get_json()
    rules=[]
    rules1=[]
    AnalysisId = content['sourceId']
    rulesetId = content['rulesetId']
    sourcepath=''
    listofNumdelays=[]
    listofTotaldelays=[]
    cdecolumns=[]
    datarules= json.loads(GetAEntityDB(AnalysisId))
    sourceObj= datarules['Analysis']
    AnalysisObj=sourceObj['source']
    sourcepath= AnalysisObj['templateSourcePath']
    for obj1 in sourceObj["rules"]:
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]
    df= df_from_path(sourcepath)        
    # file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    # if(file_ext=='.csv'):
    #     df = pd.read_csv(sourcepath)
    # elif(file_ext=='.xls'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # elif(file_ext=='.xlsx'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # else:
    #     df = pd.read_csv(sourcepath)
    cdecolumns = ["AIRLINE","FLIGHT_NUMBER","TAIL_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT","DEPARTURE_DELAY","ARRIVAL_DELAY","AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY","LATE_AIRCRAFT_DELAY","WEATHER_DELAY"]
    df1 = pd.DataFrame(df, columns=cdecolumns)
    dfs= df1.groupby(['AIRLINE'])
    for AIRLINE, frame in dfs:
        Dict = {}
        Dict['airline'] =AIRLINE
        Dict1 = {}
        Dict1['airline'] =AIRLINE
        Dictconstraints = {}
        Dictconstraints1 = {}
        Dictconstraints['NoOfDelays'] = str((frame['ARRIVAL_DELAY'] >= 15).sum() )
        Dictconstraints1['TotalDelays'] = str(frame.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
        listofNumdelays.append(Dictconstraints['NoOfDelays'])
        listofTotaldelays.append(Dictconstraints1['TotalDelays'])
        framesum= frame.groupby(['ORIGIN_AIRPORT'])
        dictList=[]
        dictList1=[]
        for ORIGIN_AIRPORT, framesub in framesum:
            Dictdet = {}
            Dictdet['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT
            Dictdet1 = {}
            Dictdet1['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT
            Dictdet['NoOfDelays'] = str((framesub['ARRIVAL_DELAY'] >= 15).sum() )
            Dictdet1['TotalDelays'] = str(framesub.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
            dictList.append(Dictdet)
            dictList1.append(Dictdet1)
        Dictconstraints['details']=dictList
        Dictconstraints1['details']=dictList1
        Dict['value'] = Dictconstraints
        Dict1['value'] = Dictconstraints1
        rules.append(Dict)
        rules1.append(Dict1)
    listofNumdelays = [int(float(s)) for s in listofNumdelays]
    graphDim={}
    graphDim['Title'] = "Airline vs #ofDelays"
    graphDim['X-Axis'] = "Airline"
    graphDim['Y-Axis'] = "#ofDelays"
    graphDim['Y-Axismin'] = str(min(listofNumdelays)-10 )
    graphDim['Y-Axismax'] = str(max(listofNumdelays)+10 )
    graphDim['Y-AxisInterval'] = '20'
    graphDim['data']= rules
    if (min(listofNumdelays)-10 <0):
        graphDim['Y-Axismin'] = 0

    listofTotaldelays = [ int(float(s))  for s in listofTotaldelays]
    graphDim1={}
    graphDim1['Title'] = "Airline vs TotalTimeDelays"
    graphDim1['X-Axis'] = "Airline"
    graphDim1['Y-Axis'] = "TotalTimeDelays"
    graphDim1['Y-Axismin'] = str(min(listofTotaldelays)-10 )
    graphDim1['Y-Axismax'] = str(max(listofTotaldelays)+10 )
    graphDim1['Y-AxisInterval'] = '200'
    graphDim1['data']= rules1
    if (min(listofTotaldelays)-10 <0):
        graphDim1['Y-Axismin'] = 0
    val=max(listofTotaldelays)
    graph=[]
    graph.append(graphDim)
    graph.append(graphDim1)

    json_data = json.dumps(graph)
    return json_data


@app.route('/api/DelayAnalysisbyAirPortnew', methods=['POST']) #GET requests will be blocked
def DelayAnalysisbyAirPortnew():

    content = request.get_json()
    rules=[]
    rules1=[]
    AnalysisId = content['sourceId']
    rulesetId = content['rulesetId']
    sourcepath=''
    listofNumdelays=[]
    listofTotaldelays=[]
    cdecolumns=[]
    datarules= json.loads(GetAEntityDB(AnalysisId))
    sourceObj= datarules['Analysis']
    AnalysisObj=sourceObj['source']
    sourcepath= AnalysisObj['templateSourcePath']
    for obj1 in sourceObj["rules"]:
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]

    # file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    # if(file_ext=='.csv'):
    #     df = pd.read_csv(sourcepath)
    # elif(file_ext=='.xls'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # elif(file_ext=='.xlsx'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # else:
    #     df = pd.read_csv(sourcepath)
    df= df_from_path(sourcepath)
    cdecolumns = ["AIRLINE","FLIGHT_NUMBER","TAIL_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT","DEPARTURE_DELAY","ARRIVAL_DELAY","AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY","LATE_AIRCRAFT_DELAY","WEATHER_DELAY"]
    df1 = pd.DataFrame(df, columns=cdecolumns)
    dfs= df1.groupby(['ORIGIN_AIRPORT'])
    for ORIGIN_AIRPORT, frame in dfs:
        Dict = {}
        Dict['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT
        Dict1 = {}
        Dict1['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT
        Dictconstraints = {}
        Dictconstraints1 = {}
        Dictconstraints['NoOfDelays'] = str((frame['ARRIVAL_DELAY'] >= 15).sum() )
        Dictconstraints1['TotalDelays'] = str(frame.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
        listofNumdelays.append(Dictconstraints['NoOfDelays'])
        listofTotaldelays.append(Dictconstraints1['TotalDelays'])
        framesum= frame.groupby(['AIRLINE'])
        dictList=[]
        dictList1=[]
        for AIRLINE, framesub in framesum:
            Dictdet = {}
            Dictdet['AIRLINE'] =AIRLINE
            Dictdet1 = {}
            Dictdet1['AIRLINE'] =AIRLINE
            Dictdet['NoOfDelays'] = str((framesub['ARRIVAL_DELAY'] >= 15).sum() )
            Dictdet1['TotalDelays'] = str(framesub.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
            dictList.append(Dictdet)
            dictList1.append(Dictdet1)
        Dictconstraints['details']=dictList
        Dictconstraints1['details']=dictList1
        Dict['value'] = Dictconstraints
        Dict1['value'] = Dictconstraints1
        rules.append(Dict)
        rules1.append(Dict1)
    listofNumdelays = [int(float(s)) for s in listofNumdelays]
    graphDim={}
    graphDim['Title'] = "Airport vs #ofDelays"
    graphDim['X-Axis'] = "Airport"
    graphDim['Y-Axis'] = "#ofDelays"
    graphDim['Y-Axismin'] = str(min(listofNumdelays)-10 )
    graphDim['Y-Axismax'] = str(max(listofNumdelays)+10 )
    graphDim['Y-AxisInterval'] = '20'
    if (min(listofNumdelays)-10 <0):
        graphDim['Y-Axismin'] = 0
    graphDim['data']= rules

    listofTotaldelays = [ int(float(s))  for s in listofTotaldelays]
    graphDim1={}
    graphDim1['Title'] = "Airport vs TotalTimeDelays"
    graphDim1['X-Axis'] = "Airport"
    graphDim1['Y-Axis'] = "TotalTimeDelays"
    graphDim1['Y-Axismin'] = str(min(listofTotaldelays)-10 )
    graphDim1['Y-Axismax'] = str(max(listofTotaldelays)+10 )
    graphDim1['Y-AxisInterval'] = '200'
    if (min(listofTotaldelays)-10 <0):
        graphDim1['Y-Axismin'] = 0
    graphDim1['data']= rules1
    val=max(listofTotaldelays)
    graph=[]
    graph.append(graphDim)
    graph.append(graphDim1)

    json_data = json.dumps(graph)
    return json_data




@app.route('/api/DelayAnalysisByAirport', methods=['POST']) #GET requests will be blocked
def DelayAnalysisByAirport():

    content = request.get_json()
    rules=[]
    AnalysisId = content['sourceId']
    rulesetId = content['rulesetId']
    sourcepath=''
    cdecolumns=[]
    datarules= json.loads(GetAEntityDB(AnalysisId))
    sourceObj= datarules['Analysis']
    AnalysisObj=sourceObj['source']
    sourcepath= AnalysisObj['templateSourcePath']
    for obj1 in sourceObj["rules"]:
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]

    # file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    # if(file_ext=='.csv'):
    #     df = pd.read_csv(sourcepath)
    # elif(file_ext=='.xls'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # elif(file_ext=='.xlsx'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # else:
    #     df = pd.read_csv(sourcepath)
    df= df_from_path(sourcepath)
    cdecolumns = ["AIRLINE","FLIGHT_NUMBER","TAIL_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT","DEPARTURE_DELAY","ARRIVAL_DELAY","AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY","LATE_AIRCRAFT_DELAY","WEATHER_DELAY"]
    df1 = pd.DataFrame(df, columns=cdecolumns)
    dfs= df1.groupby(['ORIGIN_AIRPORT'])
    for ORIGIN_AIRPORT, frame in dfs:
        Dict = {}
        Dict['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT
        Dictconstraints = {}
        Dictconstraints['NoOfDelays'] = str((frame['ARRIVAL_DELAY'] >= 15).sum() )
        Dictconstraints['TotalDelays'] = str(frame.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())

        framesum= frame.groupby(['AIRLINE'])
        dictList=[]
        for AIRLINE, framesub in framesum:
            Dictdet = {}
            Dictdet['AIRLINE'] =AIRLINE
            Dictdet['NoOfDelays'] = str((framesub['ARRIVAL_DELAY'] >= 15).sum() )
            if (framesub['ARRIVAL_DELAY'] >= 15).sum() >0:
                Dictdet['TotalDelays'] = str(framesub.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
                dictList.append(Dictdet)
        Dictconstraints['details']=dictList
        Dict['value'] = Dictconstraints
        rules.append(Dict)
    json_data = json.dumps(rules, default=str)
    return json_data

@app.route('/getLaunchMaxId', methods=['GET'])
def getLaunchMaxId(sourceId):

        with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)

        data = json_object
        IDList=[]
        IDList.append('0')
        for obj in data['LaunchAnalysis']:
                if obj["sourceId"]==sourceId:
                   for obj1 in obj["uploads"]:
                        IDList.append(obj1["launchId"][(len(sourceId)+1):] )
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))


@app.route('/getUploadMaxId', methods=['GET'])
def getUploadMaxId(sourceId):
        with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)

        data = json_object
        IDList=[]
        IDList.append('0')
        for obj in data['LaunchAnalysis']:
                if obj["sourceId"]==sourceId:
                   for obj1 in obj["uploads"]:
                        IDList.append(obj1["uploadId"][(len(sourceId)+1):] )
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))


def GetALaunchEntityDB(sourceId,uploadId):
    analysisList={}
    entiresourceList={}
    data={}
    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)

    data = json_object
    result={}
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId:
            entiresourceList=obj
            for obj1 in obj["uploads"]:
                if obj1["uploadId"]==uploadId:
                    analysisList=obj1
                    break
    result['Analysis'] = analysisList
    result['EntireSourceList'] = entiresourceList
    jsonString = json.dumps(result, default=str)
    return jsonString

def GetAEntireLaunchEntityDB(sourceId):
    analysisList={}
    entiresourceList=[]
    entiresourceObj={}
    entiresourceObj['uploads']=[]
    data={}
    with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)

    data = json_object
    result={}
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId:
            entiresourceObj=obj
        else:
            entiresourceList.append(obj)
    result['EntireSourceObj'] = entiresourceObj
    result['EntireSourceList'] = entiresourceList
    jsonString = json.dumps(result, default=str)
    return jsonString


def removeALaunchEntityDB(sourceId):
    analysisList=[]
    data={}

    with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)

    data = json_object
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]!=sourceId:
             analysisList.append(obj)
    result={}
    result['Analysis'] = analysisList
    jsonString = json.dumps(result, default=str)
    return jsonString

def GetDBentitiesforLaunch(sourceId,uploadId):
    exactuploadList={}
    SourceremovedList=[]
    exactSourceList={}
    data={}
    with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)

    data = json_object
    result={}
    IDList=[]
    IDList.append('0')
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId:
            exactSourceList=obj
            for obj1 in obj["uploads"]:
                IDList.append(obj1["launchId"][(len(sourceId)+1):] )
                if obj1["uploadId"]==uploadId:
                    exactuploadList=obj1

        else:
            SourceremovedList.append(obj)


    result['exactuploadList'] = exactuploadList
    result['SourceremovedList'] = SourceremovedList
    result['exactSourceList'] = exactSourceList
    dli2 = [int(s) for s in IDList]
    result['launchMaxId'] =  (str(max(dli2)))
    jsonString = json.dumps(result, default=str)
    return jsonString

def saveResultsets(data,name):
    #jsonString = json.dumps(data)
    filename= name + '.json'

    json.dump(data, open(filename,"w"), default=str)
    return True

@app.route('/api/getLaunchResult', methods=['GET'])
def getLaunchResult():
    sid = request.args.get('id')
    ssid =  sid.split('RS', 2)[0]
    filename= ssid + '.json'
    with open(filename, 'r') as openfile:
        json_object = json.load(openfile)

    data = json_object

    res={}
    datause=[d for d in data if (d["resultset"])==sid]
    for obj in datause:
        if obj['resultset'] == sid:
            res['result'] = obj['results']
            break
    jsonString = json.dumps(res, default=str)
    return jsonString



@app.route('/api/LaunchAnalysisbyParam', methods=['POST']) #GET requests will be blocked
def LaunchAnalysisbyParam():
    content = request.get_json()
    rules=[]
    sourceId = content['sourceId']
    rulesetId = content['rulesetId']
    KeyName = content['keyname']
    uploadId= content['uploadId']
    keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file
    rulesObject={}
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis']
    AnalysisObj=sourceObj['source']
    '''
    DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=DBList['Analysis']

    LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['Analysis']
    EntireSourceList=LaunchEntityRaw['EntireSourceList']
    sourcepath= LaunchEntity['sourcePath']
    '''
    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList']

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['exactuploadList']
    EntireSourceList=LaunchEntityRaw['exactSourceList']
    sourcepath= LaunchEntity['sourcePath']
    launchMaxId= LaunchEntity['launchMaxId']
    existingLaunchSourceList=[]
    for obj1 in EntireSourceList["uploads"]:
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= sourceId + 'L' + str(int(launchMaxId)+1)
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]
            ruleset= obj1["ruleset"]
    df= df_from_path(sourcepath)
    # file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    # if(file_ext=='.csv'):
    #     df = pd.read_csv(sourcepath)
    # elif(file_ext=='.xls'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # elif(file_ext=='.xlsx'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # else:
    #     df = pd.read_csv(sourcepath)

    df1 = pd.DataFrame(df, columns=cdecolumns)
    dfs= df1.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:
        Dict = {}
        Dict[keyv] =KeyName
        Dictconstraints = {}
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:
            DNull = {}
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            resDf = val1.where(pd.notnull(val1), 'None')

            data_dict = resDf.to_dict('index')
            count=count+1
            resultId= launchNewId +'RS'+str(count)
            DNull['Outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list


        Dict['completness'] = Dictconstraints

        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]

        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}
        DNull['column'] =str(colList)
        DNull['OutLiers'] = str(farmenew.duplicated().sum())
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        resDf1 = val.where(pd.notnull(val), 'None')

        data_dict = resDf1.to_dict('index')


        count=count+1
        resultId= launchNewId +'RS'+str(count)
        DNull['Outlier']=resultId
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)
        nulls_list.append(DNull)
        DictUnconstraints = {}
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100]
        vnulls_list=[]

        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()]
                            count=count+1
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = resDf2.to_dict('index')

                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)

                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str)
                        frame[col]=frame[col].str.replace(".", "")
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = resDf3.to_dict('index')

                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}
                        DNull['column'] =col
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]
                        count=count+1
                        resultId= launchNewId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = resDf5.to_dict('index')

                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2]
                    referenceIdRule = ru['value'].split('-', 2)[1]
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']

                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'

                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    print(refpath)
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                        csv_data = dfref.to_csv(index=None)
                        dfref = pd.read_csv(csv_data)
                    elif(file_ext=='.xlsx'):
                        dfref = pd.read_excel(refpath)
                        csv_data = dfref.to_csv(index=None)
                        dfref = pd.read_csv(csv_data)
                    else:
                        dfref = pd.read_csv(refpath)
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()
                    reftotalCount=len(frame[col])
                    refPerc= (refCount / reftotalCount)*100

                    nulls_list=[]
                    DNull = {}
                    DNull['column'] =col
                    DNull['OutLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]
                    count=count+1
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = resDf6.to_dict('index')

                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)
                    nulls_list.append(DNull)
                    DictInconstraints = {}
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints

        res= sum(value)/len(value)
        Dictvconstraints = {}
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict)
        saveResultsets(resultsetDictlist,(launchNewId))
    json_data = json.dumps(rules, default=str)
    resultobject={}
    resultobject['launchId']=launchNewId
    resultobject['results']=rules
    LaunchEntity['launchAnalysis'] = resultobject
    LaunchEntity['launchId']=launchNewId

    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity

    jsonString = json.dumps(data, default=str)
    json.dump(data, open("ldb.json","w"), default=str)
    return json.dumps(content, default=str)

@app.route('/api/LaunchAnalysisbyKeyfromDbold', methods=['POST']) #GET requests will be blocked
def LaunchAnalysisbyKeyfromDbold():

    content = request.get_json()
    rules=[]
    sourceId = content['sourceId']
    rulesetId = content['rulesetId']
    KeyName = content['keyname']
    uploadId=content['uploadId']

    analysisList={}
    entiresourceList={}
    data={}

    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)

    data = json_object
    newuploadId=''
    jsonString=''
    result={}
    isavailableflag=False
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId:
            entiresourceList=obj
            for obj1 in obj["uploads"]:
                if obj1["uploadId"]==uploadId:
                    analysisList=obj1
                    resultAlist = analysisList['launchAnalysis']
                    resultsAllList= analysisList['AnalysisResultList']

                    if not resultsAllList:
                        print('1')
                        #resultfinal=LaunchAnalysisFnCall(sourceId,rulesetId,KeyName,obj1["uploadId"])
                        content['errorMsg']= 'The processing is going on.Please try again after some time'
                        content['errorflag']= 'True'
                        content['errorCode']= '104'
                        return json.dumps(content)
                        #jsonString = json.dumps(resultfinal)
                    else:
                        print('2')
                        '''
                        if(resultAlist['keyName']== KeyName):
                            resultfinal= resultAlist['results']
                            jsonString = json.dumps(resultfinal)
                            isavailableflag =True
                        else:
                        '''
                        for resultitem in resultsAllList:
                                if(resultitem['keyName']== KeyName):
                                    resultfinal= resultitem['results']
                                    jsonString = json.dumps(resultfinal, default=str)
                                    isavailableflag =True
                                    break
                        if isavailableflag==False:
                                content['errorMsg']= 'The processing is going on.Please try again after some time'
                                content['errorflag']= 'True'
                                content['errorCode']= '104'
                                return json.dumps(content)
                    break
            if isavailableflag==False:
                                content['errorMsg']= 'The processing is going on.Please try again after some time'
                                content['errorflag']= 'True'
                                content['errorCode']= '104'
                                return json.dumps(content)
    return jsonString



@app.route('/api/LaunchAnalysisbyKeyfromDb', methods=['POST']) #GET requests will be blocked
def LaunchAnalysisbyKeyfromDbmulti():

    content = request.get_json()
    rules=[]
    sourceId = content['sourceId']
    rulesetId = content['rulesetId']
    KeyName = content['keyname']
    uploadId=content['uploadId']
    finalResults=[]
    analysisList={}
    entiresourceList={}
    data={}

    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)

    data = json_object
    newuploadId=''
    jsonString=''
    result={}
    isavailableflag=False
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId:
            entiresourceList=obj
            for obj1 in obj["uploads"]:
                if obj1["uploadId"]==uploadId:
                    analysisList=obj1
                    if KeyName=="" :
                        KeyName=getDefaultProcessedResult(analysisList)
                    sresult= getProcessedResult(analysisList,KeyName)
                    if( sresult['isException']=='No'  and  sresult['isEmpty']=='No'):
                        finalResults =finalResults+(sresult['results'])
                    elif (sresult['isException']=='No'  and  sresult['isEmpty']=='Yes'):
                        content['errorMsg']= 'The processing is going on.Please try again after some time'
                        content['errorflag']= 'True'
                        content['errorCode']= '104'
                        return json.dumps(content)
                    elif (sresult['isException']=='Yes'  and  sresult['isEmpty']=='No'):
                        content['errorMsg']= 'exception Occured!. please try reuploading the file for reprocessing'
                        content['errorflag']= 'True'
                        content['errorCode']= '107'
                        return json.dumps(content)
                    if obj1["isMultiSource"]=="Yes":
                        actualDate = datetime.strptime(obj1['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
                        for obj2 in obj["uploads"]:
                            if obj2["uploadId"]!=uploadId:
                                actualDate2 = datetime.strptime(obj2['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
                                if actualDate.date()==actualDate2.date():
                                    sresult= getProcessedResult(obj2,KeyName)
                                    if( sresult['isException']=='No'  and  sresult['isEmpty']=='No'):
                                        finalResults =finalResults+(sresult['results'])

    if not finalResults:
                        print('1')
                        #resultfinal=LaunchAnalysisFnCall(sourceId,rulesetId,KeyName,obj1["uploadId"])
                        content['errorMsg']= 'The processing is going on.Please try again after some time'
                        content['errorflag']= 'True'
                        content['errorCode']= '104'
                        return json.dumps(content)
                        #jsonString = json.dumps(resultfinal)
    else:
        fResults={}
        fResults['results']=finalResults
        fResults['keyname']=  KeyName
        jsonString = json.dumps(fResults, default=str)
    return jsonString

def getDefaultProcessedResult(analysisList):
    fname= analysisList['AnalysisResultList']
    resultfinal=[]
    filekey=''
    if fname!='':
        resultfinal= list(Path().glob(fname+"*"))
        if(len(resultfinal)>0):
            filename= resultfinal[0]
            filekey = os.path.splitext(filename)[0]
            filekey = filekey.replace(fname, '')
    return filekey

def processResult(filename):
    results={}
    resultfinal=[]
    if os.path.isfile(filename):
            with open(filename, 'r') as openfile:
                json_object = json.load(openfile)

            rsult = json_object
            resultsAllList=rsult['AnalysisResultList']
            if not resultsAllList:
                    results['isException']=    'No'
                    results['isEmpty']=    'Yes'
                    results['results']=    resultfinal
                    return results
            else:
                    resultitem= resultsAllList

                    isException=resultitem['isException']
                    if(isException=='No'):
                        #if(resultitem['keyName']== KeyName):
                        resultfinal= resultitem['results']
                        results['isException']=    'No'
                        results['isEmpty']=    'No'
                        results['results']=    resultfinal

                    else:
                        results['isException']=    'Yes'
                        results['isEmpty']=    'No'
                        results['results']=    resultfinal
                        return results
    else:
            results['isException']=    'No'
            results['isEmpty']=    'Yes'
            results['results']=    resultfinal
            return results

    return results

def getProcessedResult(analysisList,KeyName):
    fname= analysisList['AnalysisResultList']
    results={}
    resultfinal=[]
    if fname=='':
        results['isException']=    'No'
        results['isEmpty']=    'Yes'
        results['results']=    resultfinal
        return results
    else:
        filename=fname+KeyName+'.json'
        rsult={}

        # if os.path.isfile(filename):
        #     results=processResult(filename)
        # else:
        #     KeyName=getDefaultProcessedResult(analysisList)
        #     filename=KeyName
        #     results=processResult(filename)

        if os.path.isfile(filename):
            with open(filename, 'r') as openfile:
                json_object = json.load(openfile)

            rsult = json_object
            resultsAllList=rsult['AnalysisResultList']
            if not resultsAllList:
                    results['isException']=    'No'
                    results['isEmpty']=    'Yes'
                    results['results']=    resultfinal
                    return results
            else:
                    resultitem= resultsAllList

                    isException=resultitem['isException']
                    if(isException=='No'):
                        #if(resultitem['keyName']== KeyName):
                        resultfinal= resultitem['results']
                        results['isException']=    'No'
                        results['isEmpty']=    'No'
                        results['results']=    resultfinal

                    else:
                        results['isException']=    'Yes'
                        results['isEmpty']=    'No'
                        results['results']=    resultfinal
                        return results
        else:
            results['isException']=    'No'
            results['isEmpty']=    'Yes'
            results['results']=    resultfinal
            return results

    return results


def getProcessedResultgolden(analysisList,KeyName):
    fname= analysisList['AnalysisResultList']
    results={}
    resultfinal=[]
    if fname=='':
        results['isException']=    'No'
        results['isEmpty']=    'Yes'
        results['results']=    resultfinal
        return results
    else:
        filename=fname+'.json'
        print(filename)
        with open(filename, 'r') as openfile:
            json_object = json.load(openfile)

        rsult = json_object
        print(rsult)
        resultsAllList=rsult['AnalysisResultList']
        if not resultsAllList:
                results['isException']=    'No'
                results['isEmpty']=    'Yes'
                results['results']=    resultfinal
                return results
        else:
            for resultitem in resultsAllList:

                isException=resultitem['isException']
                if(isException=='No'):
                    if(resultitem['keyName']== KeyName):
                        resultfinal= resultitem['results']
                        results['isException']=    'No'
                        results['isEmpty']=    'No'
                        results['results']=    resultfinal
                        break
                else:
                    results['isException']=    'Yes'
                    results['isEmpty']=    'No'
                    results['results']=    resultfinal
                    return results

    return results

@app.route('/api/GetTimeResultsfromDb', methods=['POST']) #GET requests will be blocked
def GetTimeResultsfromDb():

    content = request.get_json()
    rules=[]
    sourceId = content['sourceId']
   
    KeyName = content['keyname']
    finalResults=[]
    analysisList={}
    entiresourceList={}
    data={}

    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)

    data = json_object
    newuploadId=''
    jsonString=''
    result={}
    isavailableflag=False
    processedResult=[]
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId:
            entiresourceList=obj
            for obj1 in obj["uploads"]:
                    if obj1["isMultiSource"]=="Yes":
                        print('dateList')
                        sresult= getAggregateformultisource(obj["uploads"],KeyName)
                        jsonString = json.dumps(sresult, default=str)
                        return jsonString
                    else:
                        UploadObjDict={}
                        print('dateList1')
                        if KeyName=="" :
                            KeyName=getDefaultProcessedResult(obj1)
                        sresult= getProcessedAggResult(obj1,KeyName)
                        print(sresult)
                        if( sresult['isException']=='No'  and  sresult['isEmpty']=='No'):
                            finalResults =(sresult['results'])
                            UploadObjDict['uploadDate']= obj1['uploadDate']
                            UploadObjDict['results']=finalResults
                            processedResult.append(UploadObjDict)

    if not processedResult:
                        print('1')
                        #resultfinal=LaunchAnalysisFnCall(sourceId,rulesetId,KeyName,obj1["uploadId"])
                        content['errorMsg']= 'The processing is going on.Please try again after some time'
                        content['errorflag']= 'True'
                        content['errorCode']= '104'
                        return json.dumps(content)
                        #jsonString = json.dumps(resultfinal)
    else:
        jsonString = json.dumps(processedResult, default=str)
    return jsonString


def getProcessedAggResult(analysisList,KeyName):
    print(analysisList)
    fname= analysisList['AnalysisResultList']
    results={}
    resultfinal=[]
    if fname=='':
        results['isException']=    'No'
        results['isEmpty']=    'Yes'
        results['results']=    resultfinal
        return results
    else:
        filename=fname+KeyName+'.json'
        print(filename)
        if os.path.isfile(filename):
            print("file exist")
            with open(filename, 'r') as openfile:
                json_object = json.load(openfile)
            print(json_object)
            rsult = json_object
            resultsAllList=rsult['AnalysisResultList']
            print('analysisResult')
            print(resultsAllList)
            if not resultsAllList:
                    print('empty loop')
                    results['isException']=    'No'
                    results['isEmpty']=    'Yes'
                    results['results']=    resultfinal
                    return results
            else:
                    resultitem = resultsAllList

                    isException=resultitem['isException']
                    if(isException=='No'):
                        if(resultitem['keyName']== KeyName):
                            resultfinal= resultitem['aggResults']
                            results['isException']=    'No'
                            results['isEmpty']=    'No'
                            results['results']=    resultfinal

                    else:
                        results['isException']=    'Yes'
                        results['isEmpty']=    'No'
                        results['results']=    resultfinal
                        return results
        else:
            print("file not exist")
            results['isException']=    'No'
            results['isEmpty']=    'Yes'
            results['results']=    resultfinal
            return results
    return results


def getAggregateformultisource(obj,KeyName):
    dateList=[]
    processedResult=[]
    for obj1 in obj:
        actualDate = datetime.strptime(obj1['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
        dateList.append(actualDate.date())
    test_list = list(set(dateList))
    for eachdate in test_list:
        completeness=[]
        Accuracy=[]
        Uniqueness=[]
        Integrity=[]
        Validity=[]
        for obj2 in obj:
            actualDate2 = datetime.strptime(obj2['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
            if eachdate==actualDate2.date():
                sresult= getProcessedAggResult(obj2,KeyName)
                if( sresult['isException']=='No'  and  sresult['isEmpty']=='No'):
                    finalResults =(sresult['results'])
                    if 'Completeness' in finalResults:
                        completeness.append(float(finalResults['Completeness']))
                    if 'Accuracy' in finalResults:
                        Accuracy.append(float(finalResults['Accuracy']))
                    if 'Uniqueness' in finalResults:
                        Uniqueness.append(float(finalResults['Uniqueness']))
                    if 'Integrity' in finalResults:
                        Integrity.append(float(finalResults['Integrity']))
                    if 'Validity' in finalResults:
                        Validity.append(float(finalResults['Validity']))
        UploadObjDict={}
        UploadObjDict['uploadDate']= eachdate

        DictFinal={}
        if len(completeness)>0:
              cPerc= sum(completeness)/len(completeness)
              DictFinal['Completeness'] = str(round(cPerc, 2))
        if len(Accuracy)>0:
              cPerc= sum(Accuracy)/len(Accuracy)
              DictFinal['Accuracy'] = str(round(cPerc, 2))
        if len(Uniqueness)>0:
              cPerc= sum(Uniqueness)/len(Uniqueness)
              DictFinal['Uniqueness'] = str(round(cPerc, 2))
        if len(Integrity)>0:
              cPerc= sum(Integrity)/len(Integrity)
              DictFinal['Integrity'] = str(round(cPerc, 2))
        if len(Validity)>0:
              cPerc= sum(Validity)/len(Validity)
              DictFinal['Validity'] = str(round(cPerc, 2))
        UploadObjDict['results']=DictFinal
        processedResult.append(UploadObjDict)
    return processedResult


@app.route('/api/uploadSourceold', methods=['POST'])
def uploadSourceold():
    uploaded_files = request.files.getlist("file[]")
    content =json.loads(request.form.get('data'))
    sourceId= content['sourceId']
    rulesetId= content['rulesetId']
    isMultiSource= content['isMultiSource']
    multiSourceKey= content['multiSourceKey']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['sourceObj']

    settings= content['settings']
    sourceFilename = sourcedetails['sourceFileName']
    sourceCatColumns = sourcedetails['categorialColumns']
    sourcePath=''
    keyNametoLaunch= sourceCatColumns[0]
    newuploadId=sourceId+'U'+ str(int(getUploadMaxId(sourceId))+1)
    sourceFilename=newuploadId + str(uploadDate)+ sourceFilename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB(sourceId))

    EntireSourceObj= LaunchEntityRaw['EntireSourceObj']
    EntireSourceList= LaunchEntityRaw['EntireSourceList']
    uploadsObject= EntireSourceObj['uploads']

    print(uploadReason)
    if(uploadReason==''):
        if(isMultiSource=="Yes"):
            if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                        multisrObj= item['multiSourcePath']
                        if multisrObj['multiSourceKey']==multiSourceKey:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file for this multisource key is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
        else:
                if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)

    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
                file_ext = os.path.splitext(filename)[1]
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    dli=[]
                    sourcePath=sourceFilename
                    file.save(sourcePath)
                    # if(file_ext=='.csv'):
                    #     df = pd.read_csv(sourcePath)
                    # elif(file_ext=='.xls'):
                    #     df = pd.read_excel(sourcePath)
                    #     csv_data = df.to_csv(index=None)
                    #     df = pd.read_csv(StringIO(csv_data))
                    # elif(file_ext=='.xlsx'):
                    #     df = pd.read_excel(sourcePath)
                    #     csv_data = df.to_csv(index=None)
                    #     df = pd.read_csv(StringIO(csv_data))
                    # else:
                    #     df = pd.read_csv(sourcePath)
                    df= df_from_path(sourcePath)
                    dli= list(df.columns.values)
                    sourcedetails['availableColumns'] = dli
                    dli=dli.sort()
                    sourceavailableColumns=sourcedetails['availableColumns']
                    sourceavailableColumns=sourceavailableColumns.sort()
                    if dli != sourceavailableColumns:
                        content['errorMsg']= 'The file headers does not match with the configured source'
                        content['errorflag']= 'True'
                        content['errorCode']= '103'
                        return json.dumps(content)
    resultContent={}

    resultContent['launchId']=newuploadId
    resultContent['uploadId']=newuploadId
    resultContent['rulesetId']=rulesetId
    resultContent['sourceFileName']=sourceFilename
    resultContent['sourcePath']=sourcePath
    resultContent['uploadDate']=uploadDate
    resultContent['uploadTime']=uploadTime
    resultContent['isexpecteddate']='Yes'
    resultContent['reference']=[]
    resultContent['launchAnalysis']={}
    resultList=[]
    '''for objcol in sourceCatColumns:
        resultobject={}
        resultobject['launchId']=''
        resultobject['keyName']=objcol
        resultobject['results']=[]
        resultList.append(resultobject)
    '''
    resultContent['AnalysisResultList']=''
    resultContent['isMultiSource']=isMultiSource
    resultContent['multiSource']=[]
    #multiSourcePaths=[]
    if(isMultiSource=="Yes"):
        mpath={}
        mpath['sourcePath']=  sourcePath
        mpath['multiSourceKey']=multiSourceKey
        #multiSourcePaths.append(mpath)
        resultContent['multiSourcePath']=mpath
    content['uploadId']=newuploadId
    content['sourceFilename']=sourceFilename
    uploadsObject.append(resultContent)
    EntireSourceObj['uploads']=uploadsObject
    EntireSourceObj['sourceId']=sourceId
    EntireSourceList.append(EntireSourceObj)

    data={}
    data['LaunchAnalysis']=EntireSourceList
    jsonString= json.dumps(data, default=str)
    json.dump(data, open("ldb.json","w"), default=str)
    content1={}
    content1['errorMsg']= ''
    content1['errorflag']= 'False'
    content1['responseObj']= jsonString

    tempDict={}
    tempDict['sourceId']= sourceId
    tempDict['uploadId'] =newuploadId
    tempDict['source']= sourcedetails
    uploadSourceDetail= {}
    uploadSourceDetail['uploadId'] =newuploadId
    uploadSourceDetail['isMultiSource'] =isMultiSource
    uploadSourceDetail['uploadDate'] =uploadDate
    uploadSourceDetail['uploadTime'] =uploadTime
    uploadSourceDetail['sourceFileName']=sourceFilename
    tempDict['rules']=[]
    tempDict["UploadsHistory"]= []
    tempDict['recentsourceUpload'] = uploadSourceDetail


    inputcontent1={}
    inputcontent1['sourceId']=sourceId
    inputcontent1['uploadId']=newuploadId
    inputcontent1['rulesetId']=rulesetId
    inputcontent1['keyname']=keyNametoLaunch

    print(keyNametoLaunch)




    def long_running_task(**kwargs):
        inputcontent = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", inputcontent)
        rules=[]
        sourceId = inputcontent['sourceId']
        rulesetId = inputcontent['rulesetId']
        KeyName = inputcontent['keyname']
        uploadId=inputcontent['uploadId']
        LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId)

        print('Completed the main thread function')
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': inputcontent1})
    thread.start()

    return json.dumps(tempDict, default=str)

@app.route('/api/uploadSourcenew', methods=['POST'])
def uploadSourcenew():
    uploaded_files = request.files.getlist("file[]")
    content =json.loads(request.form.get('data'))
    sourceId= content['sourceId']
    rulesetId= content['rulesetId']
    isMultiSource= content['isMultiSource']
    multiSourceKey= content['multiSourceKey']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['sourceObj']

    settings= content['settings']
    sourceFilename = sourcedetails['sourceFileName']
    sourceCatColumns = sourcedetails['categorialColumns']
    sourcePath=''
    keyNametoLaunch= sourceCatColumns[0]
    newuploadId=sourceId+'U'+ str(int(getUploadMaxId(sourceId))+1)
    sourcename = os.path.splitext(sourceFilename)[0]
    print(sourcename)
    sourceFilename=newuploadId + str(uploadDate)+ sourcename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB(sourceId))

    EntireSourceObj= LaunchEntityRaw['EntireSourceObj']
    EntireSourceList= LaunchEntityRaw['EntireSourceList']
    uploadsObject= EntireSourceObj['uploads']

    print(uploadReason)
    if(uploadReason==''):
        if(isMultiSource=="Yes"):
            if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                        multisrObj= item['multiSourcePath']
                        if multisrObj['multiSourceKey']==multiSourceKey:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file for this multisource key is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content, default=str)
        else:
                if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)

    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
                print(sourceFilename)
                file_ext = os.path.splitext(filename)[1]
                sourceFilename=sourceFilename+file_ext
                print(file_ext)
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    dli=[]
                    sourcePath=sourceFilename
                    file.save(sourcePath)
                    # if(file_ext=='.csv'):
                    #     df = pd.read_csv(sourcePath)
                    # elif(file_ext=='.xls'):
                    #     df = pd.read_excel(sourcePath)
                    #     csv_data = df.to_csv(index=None)
                    #     df = pd.read_csv(StringIO(csv_data))
                    # elif(file_ext=='.xlsx'):
                    #     df = pd.read_excel(sourcePath)
                    #     csv_data = df.to_csv(index=None)
                    #     df = pd.read_csv(StringIO(csv_data))
                    # else:
                    #     df = pd.read_csv(sourcePath)
                    df= df_from_path(sourcePath)
                    dli= list(df.columns.values)
                    sourcedetails['availableColumns'] = dli
                    dli=dli.sort()
                    sourceavailableColumns=sourcedetails['availableColumns']
                    sourceavailableColumns=sourceavailableColumns.sort()
                    if dli != sourceavailableColumns:
                        content['errorMsg']= 'The file headers does not match with the configured source'
                        content['errorflag']= 'True'
                        content['errorCode']= '103'
                        return json.dumps(content)
    resultContent={}

    resultContent['launchId']=newuploadId
    resultContent['uploadId']=newuploadId
    resultContent['rulesetId']=rulesetId
    resultContent['sourceFileName']=sourceFilename
    resultContent['sourcePath']=sourcePath
    resultContent['uploadDate']=uploadDate
    resultContent['uploadTime']=uploadTime
    resultContent['isexpecteddate']='Yes'
    resultContent['reference']=[]
    resultContent['launchAnalysis']={}
    resultList=[]

    resultContent['AnalysisResultList']=''
    resultContent['isMultiSource']=isMultiSource
    resultContent['multiSource']=[]
    #multiSourcePaths=[]
    if(isMultiSource=="Yes"):
        mpath={}
        mpath['sourcePath']=  sourcePath
        mpath['multiSourceKey']=multiSourceKey
        #multiSourcePaths.append(mpath)
        resultContent['multiSourcePath']=mpath
    content['uploadId']=newuploadId
    content['sourceFilename']=sourceFilename
    uploadsObject.append(resultContent)
    EntireSourceObj['uploads']=uploadsObject
    EntireSourceObj['sourceId']=sourceId
    EntireSourceList.append(EntireSourceObj)

    data={}
    data['LaunchAnalysis']=EntireSourceList
    jsonString= json.dumps(data, default=str)
    json.dump(data, open("ldb.json","w"), default=str)
    content1={}
    content1['errorMsg']= ''
    content1['errorflag']= 'False'
    content1['responseObj']= jsonString

    tempDict={}
    tempDict['sourceId']= sourceId
    tempDict['uploadId'] =newuploadId
    tempDict['source']= sourcedetails
    uploadSourceDetail= {}
    uploadSourceDetail['uploadId'] =newuploadId
    uploadSourceDetail['isMultiSource'] =isMultiSource
    uploadSourceDetail['uploadDate'] =uploadDate
    uploadSourceDetail['uploadTime'] =uploadTime
    uploadSourceDetail['sourceFileName']=sourceFilename
    tempDict['rules']=[]
    tempDict["UploadsHistory"]= []
    tempDict['recentsourceUpload'] = uploadSourceDetail


    inputcontent1={}
    inputcontent1['sourceId']=sourceId
    inputcontent1['uploadId']=newuploadId
    inputcontent1['rulesetId']=rulesetId
    inputcontent1['keyname']=sourceCatColumns




    def long_running_task(**kwargs):
        inputcontent = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", inputcontent)
        rules=[]
        sourceId = inputcontent['sourceId']
        rulesetId = inputcontent['rulesetId']
        KeyName = inputcontent['keyname']
        uploadId=inputcontent['uploadId']
        #LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId)
        LaunchAnalysisthread(sourceId,rulesetId,KeyName,uploadId)
        print('Completed the main thread function')

    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': inputcontent1})
    thread.start()

    return json.dumps(tempDict, default=str)


def LaunchAnalysisFnCall(sourceId,rulesetId,KeyName,uploadId):
    print('starting long running task')
    rules=[]
    keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file
    rulesObject={}
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis']
    AnalysisObj=sourceObj['source']

    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList']

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['exactuploadList']
    EntireSourceList=LaunchEntityRaw['exactSourceList']
    sourcepath= LaunchEntity['sourcePath']
    AnalysisResultList= LaunchEntity['AnalysisResultList']
    multiSourceKey=""
    if(LaunchEntity['isMultiSource']=='Yes'):
        multiSources= LaunchEntity['multiSourcePath']
        sourcepath=multiSources['sourcePath']
        multiSourceKey=multiSources['multiSourceKey']
    else:
        sourcepath= LaunchEntity['sourcePath']
        multiSourceKey=""
    launchMaxId= LaunchEntityRaw['launchMaxId']
    existingLaunchSourceList=[]
    for obj1 in EntireSourceList["uploads"]:
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= sourceId + 'L' + str(int(launchMaxId)+1)
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]
            ruleset= obj1["ruleset"]
    df= df_from_path(sourcepath)
    # file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    # if(file_ext=='.csv'):
    #     df = pd.read_csv(sourcepath)
    # elif(file_ext=='.xls'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # elif(file_ext=='.xlsx'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # else:
    #     df = pd.read_csv(sourcepath)

    df1 = pd.DataFrame(df, columns=cdecolumns)
    df2 = df1.where(pd.notnull(df1), 'None')
    dfs= df2.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:
        Dict = {}
        Dict[keyv] =KeyName
        Dictconstraints = {}
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:
            DNull = {}
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            #resDf = val1.where(pd.notnull(val1), 'None')

            data_dict = val1.to_dict('index')
            count=count+1
            resultId= launchNewId +'RS'+str(count)
            DNull['outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list


        Dict['completness'] = Dictconstraints

        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]

        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}
        DNull['column'] =str(colList)
        DNull['outLiers'] = str(farmenew.duplicated().sum())
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        #resDf1 = val.where(pd.notnull(val1), 'None')

        data_dict = val.to_dict('index')


        count=count+1
        resultId= launchNewId +'RS'+str(count)
        DNull['outlier']=resultId
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)
        nulls_list.append(DNull)
        DictUnconstraints = {}
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100]
        vnulls_list=[]

        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()]
                            count=count+1
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            #resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = r3.to_dict('index')

                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)

                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str)
                        frame[col]=frame[col].str.replace(".", "")
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            #resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = r4.to_dict('index')

                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}
                        DNull['column'] =col
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]
                        count=count+1
                        resultId= launchNewId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        #resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = r5.to_dict('index')

                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2]
                    referenceIdRule = ru['value'].split('-', 2)[1]
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']

                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'

                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    print(refpath)
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                        csv_data = dfref.to_csv(index=None)
                        dfref = pd.read_csv(csv_data)
                    elif(file_ext=='.xlsx'):
                        dfref = pd.read_excel(refpath)
                        csv_data = dfref.to_csv(index=None)
                        dfref = pd.read_csv(csv_data)
                    else:
                        dfref = pd.read_csv(refpath)
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()
                    reftotalCount=len(frame[col])
                    refPerc= (refCount / reftotalCount)*100

                    nulls_list=[]
                    DNull = {}
                    DNull['column'] =col
                    DNull['outLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]
                    count=count+1
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    #resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = r6.to_dict('index')

                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)
                    nulls_list.append(DNull)
                    DictInconstraints = {}
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints

        res= sum(value)/len(value)
        Dictvconstraints = {}
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict)
        saveResultsets(resultsetDictlist,(launchNewId))
    #json_data = json.dumps(rules)
    resultobject={}
    resultobject['launchId']=launchNewId
    resultobject['keyName']=KeyName
    resultobject['results']=rules
    #LaunchEntity['launchAnalysis'] = resultobject
    LaunchEntity['launchId']=launchNewId
    AnalysisResultList.append(resultobject)
    LaunchEntity['AnalysisResultList']=AnalysisResultList
    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity

    jsonString = json.dumps(data, default=str)
    json.dump(data, open("ldb.json","w"), default=str)

    return rules

def LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId):
    print('starting long running task')
    rules=[]
    keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file
    rulesObject={}
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis']
    AnalysisObj=sourceObj['source']

    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList']

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['exactuploadList']
    EntireSourceList=LaunchEntityRaw['exactSourceList']
    sourcepath= LaunchEntity['sourcePath']
    multiSourceKey=""
    if(LaunchEntity['isMultiSource']=='Yes'):
        multiSources= LaunchEntity['multiSourcePath']
        sourcepath=multiSources['sourcePath']
        multiSourceKey=multiSources['multiSourceKey']
    else:
        sourcepath= LaunchEntity['sourcePath']
        multiSourceKey=""
    AnalysisResultList= LaunchEntity['AnalysisResultList']
    launchMaxId= LaunchEntityRaw['launchMaxId']
    existingLaunchSourceList=[]
    for obj1 in EntireSourceList["uploads"]:
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= sourceId + 'L' + str(int(launchMaxId)+1)
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]
            ruleset= obj1["ruleset"]
    # file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    # if(file_ext=='.csv'):
    #     df = pd.read_csv(sourcepath)
    # elif(file_ext=='.xls'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # elif(file_ext=='.xlsx'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # else:
    #     df = pd.read_csv(sourcepath)
    df= df_from_path(sourcepath)
    df1 = pd.DataFrame(df, columns=cdecolumns)
    df2 = df1.where(pd.notnull(df1), 'None')
    dfs= df2.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:
        Dict = {}
        Dict[keyv] =KeyName
        Dictconstraints = {}
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:
            DNull = {}
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            #resDf = val1.where(pd.notnull(val1), 'None')

            data_dict = val1.to_dict('index')
            count=count+1
            resultId= launchNewId +'RS'+str(count)
            DNull['outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list


        Dict['completness'] = Dictconstraints

        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]

        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}
        DNull['column'] =str(colList)
        DNull['outLiers'] = str(farmenew.duplicated().sum())
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        #resDf1 = val.where(pd.notnull(val1), 'None')

        data_dict = val.to_dict('index')


        count=count+1
        resultId= launchNewId +'RS'+str(count)
        DNull['outlier']=resultId
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)
        nulls_list.append(DNull)
        DictUnconstraints = {}
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100]
        vnulls_list=[]

        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()]
                            count=count+1
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            #resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = r3.to_dict('index')

                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)

                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str)
                        frame[col]=frame[col].str.replace(".", "",regex=False)
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            #resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = r4.to_dict('index')

                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}
                        DNull['column'] =col
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]
                        count=count+1
                        resultId= launchNewId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        #resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = r5.to_dict('index')

                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2]
                    referenceIdRule = ru['value'].split('-', 2)[1]
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']

                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'
                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    print(refpath)
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                        csv_data = dfref.to_csv(index=None)
                        dfref = pd.read_csv(csv_data)
                    elif(file_ext=='.xlsx'):
                        dfref = pd.read_excel(refpath)
                        csv_data = dfref.to_csv(index=None)
                        dfref = pd.read_csv(csv_data)
                    else:
                        dfref = pd.read_csv(refpath)
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()
                    reftotalCount=len(frame[col])
                    refPerc= (refCount / reftotalCount)*100

                    nulls_list=[]
                    DNull = {}
                    DNull['column'] =col
                    DNull['outLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]
                    count=count+1
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    #resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = r6.to_dict('index')

                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)
                    nulls_list.append(DNull)
                    DictInconstraints = {}
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints

        res= sum(value)/len(value)
        Dictvconstraints = {}
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict)
        saveResultsets(resultsetDictlist,(launchNewId))

    json_data = json.dumps(rules, default=str)
    resultobject={}
    resultobject['launchId']=launchNewId
    resultobject['keyName']=keyv
    resultobject['results']=rules
    LaunchEntity['launchAnalysis'] = resultobject
    LaunchEntity['launchId']=launchNewId
    print('before' + keyv )
    print(AnalysisResultList)
    AnalysisResultList.append(resultobject)
    LaunchEntity['AnalysisResultList']=AnalysisResultList

    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity
    jsonString = json.dumps(data, default=str)
    json.dump(data, open("ldb.json","w"), default=str)
    print('Completed for' + keyv )
    return 'Completed'

@app.route('/registrazione', methods=['GET', 'POST'])
def start_task():
    content1 = request.get_json()
    rules=[]
    sourceId = content1['sourceId']
    print(sourceId)
    def long_running_task(**kwargs):
        content = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", content)
        rules=[]
        sourceId = content['sourceId']
        rulesetId = content['rulesetId']
        KeyName = content['keyname']
        uploadId=content['uploadId']
        saveResultsets(content,'testResultset')

        print('Completed the main thread function')
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': content1})
    thread.start()
    thread.join()
    return 'started'


@app.route('/api/getAllSources', methods=['GET'])
def getAllSources():
    analysisList=[]
    data={}
    with open('db.json', 'r') as openfile:
        json_object = json.load(openfile)
    data = json_object

    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)

    dataldb = json_object
    LDBData = dataldb['LaunchAnalysis']

    with open('cleandb.json', 'r') as openfile:
        json_object = json.load(openfile)

    cleandb_data = json_object["SourceDetailsList"]

    with open('profiledb.json', 'r') as openfile:
        profiledb_json_object = json.load(openfile)

    for obj in data['Analysis']:
        tempDict={}
        temprules=[]
        uploadSourceList=[]
        cleanedhistList = []
        recentsourceUpload={}
        tempDict['sourceId']= obj['sourceId']
        sourceId= obj['sourceId']
        tempDict['source']= obj['source']
        sourceData= obj['source']
        tempDict['reference']= obj['reference']
        tempDict['settings']=obj['settings']
        for objldb in LDBData:
            if objldb["sourceId"]==sourceId:
                for objldb1 in objldb["uploads"]:
                    uploadSourceDetail= {}
                    uploadSourceDetail['uploadId'] =objldb1["uploadId"]
                    uploadSourceDetail['isMultiSource'] =objldb1["isMultiSource"]
                    if(objldb1["isMultiSource"]=="Yes"):
                        multiSourcePathObj= objldb1['multiSourcePath']
                        uploadSourceDetail['multiSourceKey'] =multiSourcePathObj["multiSourceKey"]
                    uploadSourceDetail['uploadDate'] =objldb1["uploadDate"]
                    uploadSourceDetail['uploadTime'] =objldb1["uploadTime"]
                    uploadSourceDetail['sourceFileName']=objldb1["sourceFileName"]
                    uploadSourceList.append(uploadSourceDetail)
                    recentsourceUpload=uploadSourceDetail
                for objcleandb in cleandb_data:
                    if objcleandb["sourceId"]==sourceId:
                        cleanedhistList.append(objcleandb)

        for obj1 in obj["rules"]:
            temprulesDict={}
            temprulesDict['sourceId']= obj['sourceId']
            temprulesDict["rulesetId"]=obj1["rulesetId"]
            temprulesDict["selectedColumns"]=    obj1["selectedColumns"]
            #temprulesDict["Referencepath"]=    obj1["Referencepath"]
            temprulesDict["refSelectedColumns"]=    obj1["refSelectedColumns"]
            temprulesDict['startDate'] = obj1['startDate']
            temprulesDict['endDate'] = obj1['endDate']
            temprulesDict["ruleset"]= obj1["ruleset"]
            temprulesDict["rulesetName"]= obj1["rulesetName"]
            temprulesDict["columns"]= sourceData["availableColumns"]

            startDate_obj =  datetime.strptime(obj1['startDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
            endDate_obj = datetime.strptime(obj1['endDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

            if startDate_obj.date() <= date.today() <= endDate_obj.date():
                temprulesDict["Rulestatus"]= "Active"
            elif startDate_obj.date() >= date.today():
                temprulesDict["Rulestatus"]= "Inactive"
            else:
                temprulesDict["Rulestatus"]= 'Expired'

            temprules.append(temprulesDict)
        tempDict['rules']=temprules
        tempDict["UploadsHistory"]= uploadSourceList
        tempDict["CleanedHistory"] = cleanedhistList
        tempDict["ProfiledBHistory"] = profiledb_json_object["SourceDetailsList"]
        tempDict['recentsourceUpload'] = recentsourceUpload
        analysisList.append(tempDict)
    data['Analysis'] = analysisList
    jsonString = json.dumps(data, default=str)
    return jsonString
    
@app.route('/api/getAllDetails', methods=['POST'])
def getAllDetails():

    content = request.get_json()    
    userName=content['userName']
    userCategory=content['userCategory']
    userDepartment= content['department']

    analysisList=[]
    data={}
    with open('db.json', 'r') as openfile:
        json_object = json.load(openfile)
    data = json_object

    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)

    dataldb = json_object
    LDBData = dataldb['LaunchAnalysis']

    with open('cleandb.json', 'r') as openfile:
        json_object = json.load(openfile)

    cleandb_data = json_object["SourceDetailsList"]

    with open('profiledb.json', 'r') as openfile:
        profiledb_json_object = json.load(openfile)
    

    for obj in data['Analysis']:
        if isSourceValid(userName,userCategory,userDepartment,obj) :
            tempDict={}
            temprules=[]
            uploadSourceList=[]
            cleanedhistList = []
            recentsourceUpload={}
            tempDict['sourceId']= obj['sourceId']
            sourceId= obj['sourceId']
            tempDict['source']= obj['source']
            sourceData= obj['source']
            tempDict['reference']= obj['reference']
            tempDict['settings']=obj['settings']
            datedHistory=[]
            datedHistorycat=[]
            LDBDataList=[]
            LDBDataList=[d for d in LDBData if (d["sourceId"])==sourceId]
            for objldb in LDBDataList:
                    dateList=[]
                    dateLitnew=[]
                    dateLitnew=(([((datetime.strptime(d['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ"))).date() for d in objldb["uploads"] if d["isCleanedSource"] !="YES"]))                
                    test_list = list(set(dateLitnew))
                    for eachdate in test_list:                    
                        datelist= [d for d in objldb["uploads"] if (d["isCleanedSource"] !="YES" and (datetime.strptime(d["uploadDate"],"%Y-%m-%dT%H:%M:%S.%fZ")).date()==eachdate)] 
                        #li= [d for d in datelist if (d["isCleanedSource"])=="YES"] 
                        dateuploadList={}
                        dateuploadList['uploadDate'] =eachdate
                        uploadLIst=[]
                        dateuploadListcat={}
                        dateuploadListcat['uploadDate'] =eachdate
                        uploadLIstcat=[]
                        datelistnew=datelist
                        # print(eachdate)
                        # print('before filtering')
                        # print(datelist)
                        # if len(datelist)>1:
                        #     maxidclean= getmaxuploadId(datelist,sourceId,"clean")
                        #     maxidNormal=getmaxuploadId(datelist,sourceId,"normal")

                        #     maxidclean=sourceId+'U'+ str(int(maxidclean))
                        #     maxidNormal=sourceId+'U'+ str(int(maxidNormal))
                        #     maxid=[maxidclean,maxidNormal]
                        datelistnew=[d for d in datelist if d["isCleanedSource"] !="YES"]
                        datedCatecoryList=[] 
                        d={}
                        for objldb1 in datelistnew:
                                detailsDict={}   
                                detailsDict=getDictforDetail(objldb1)
                                uploadSourceDetail=detailsDict
                                datelistCleanedNew=[d for d in objldb["uploads"] if (d["isCleanedSource"] =="YES" and d["uploadSourceId"] ==objldb1["uploadId"] )]
                                cleanList=[]
                                
                                uniqueSourcepath=[d['sourcePath'] for d in datelistCleanedNew if d["isCleanedSource"] =="YES" ]
                                uniqueSourcepath1=list(set(uniqueSourcepath))
                                newCleanedList=[]
                                for eachsourceCleanedPath in  uniqueSourcepath1:
                                    cleanedListPerPath=[d for d in datelistCleanedNew if d["sourcePath"]==eachsourceCleanedPath ]
                                    maxidclean= getmaxuploadId(cleanedListPerPath,sourceId,"clean")
                                    maxidclean=sourceId+'U'+ str(int(maxidclean))
                                    cleanedListPerPathNew=[d for d in datelistCleanedNew if d["uploadId"]==maxidclean ]
                                    newCleanedList=newCleanedList+(cleanedListPerPathNew)

                                print("printing new cleaned filde")
                                print(newCleanedList)    
                                for cleanUpload in newCleanedList:
                                     Cl_dict={}
                                     Cl_dict=getDictforDetail(cleanUpload)
                                     cleanList.append(Cl_dict)
                                detailsDict["cleanedSources"]=cleanList     
                                uploadSourceList.append(uploadSourceDetail)
                                uploadLIst.append(detailsDict)
                                
                                recentsourceUpload=uploadSourceDetail
                        d_modified={}
                        print(d)
                        if("NormalSource" in d.keys()):
                            d_modified=d["NormalSource"]
                        if("CleanedSource" in d.keys()):    
                            d_modified["CleanedSource"] =  d["isCleanedSource"]
                        uploadLIstcat.append(d_modified)        
                        dateuploadList['uploadDetails'] =uploadLIst
                        dateuploadListcat['uploadDetails']=uploadLIstcat
                        datedHistory.append(dateuploadList)
                        datedHistorycat.append(dateuploadListcat)
                    for objcleandb in cleandb_data:
                        if objcleandb["sourceId"]==sourceId or objcleandb["originalsourceId"] ==sourceId:
                            cleanedhistList.append(objcleandb)

            for obj1 in obj["rules"]:
                temprulesDict={}
                temprulesDict['sourceId']= obj['sourceId']
                temprulesDict["rulesetId"]=obj1["rulesetId"]
                temprulesDict["selectedColumns"]=    obj1["selectedColumns"]
                #temprulesDict["Referencepath"]=    obj1["Referencepath"]
                temprulesDict["refSelectedColumns"]=    obj1["refSelectedColumns"]
                temprulesDict['startDate'] = obj1['startDate']
                temprulesDict['endDate'] = obj1['endDate']
                temprulesDict["ruleset"]= obj1["ruleset"]
                temprulesDict["rulesetName"]= obj1["rulesetName"]
                temprulesDict['rulesfor2cols']= obj1['rulesfor2cols']
                temprulesDict['rulesfor3cols']= obj1['rulesfor3cols']
                temprulesDict['rulesformulticols']= obj1['rulesformulticols']
                temprulesDict["columns"]= sourceData["availableColumns"]

                startDate_obj =  datetime.strptime(obj1['startDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
                endDate_obj = datetime.strptime(obj1['endDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                if startDate_obj.date() <= date.today() <= endDate_obj.date():
                    temprulesDict["Rulestatus"]= "Active"
                elif startDate_obj.date() >= date.today():
                    temprulesDict["Rulestatus"]= "Inactive"
                else:
                    temprulesDict["Rulestatus"]= 'Expired'

                temprules.append(temprulesDict)
            tempDict['rules']=temprules
            tempDict["UploadsHistory"]= uploadSourceList
            tempDict["DatedUploadHistory"]=datedHistory
            tempDict["DatedCategoryUploadHistory"]=datedHistorycat
            tempDict["CleanedHistory"] = cleanedhistList
            tempDict["ProfiledBHistory"] = profiledb_json_object["SourceDetailsList"]
            tempDict['recentsourceUpload'] = recentsourceUpload
            analysisList.append(tempDict)
    data['Analysis'] = analysisList
    jsonString = json.dumps(data, default=str)
    return jsonString

def getDictforDetail(objldb1):
                                detailsDict={}   
                                detailsCAteoryDict={}                          
                                uploadSourceDetail= {}
                                uploadSourceDetail['uploadId'] =objldb1["uploadId"]
                                uploadSourceDetail['isMultiSource'] =objldb1["isMultiSource"]
                                if(objldb1["isMultiSource"]=="Yes"):
                                    multiSourcePathObj= objldb1['multiSourcePath']
                                    uploadSourceDetail['multiSourceKey'] =multiSourcePathObj["multiSourceKey"]
                                uploadSourceDetail['uploadDate'] =objldb1["uploadDate"]
                                uploadSourceDetail['uploadTime'] =objldb1["uploadTime"]
                                uploadSourceDetail['sourceFileName']=objldb1["sourceFileName"]
                                if(objldb1["sourceFileName"]!=""):
                                    if "cleaned_data/" in objldb1["sourceFileName"]:
                                        x = objldb1["sourceFileName"].strip("cleaned_data/")
                                        uploadSourceDetail['sourceFileName']=x
                                uploadSourceDetail['sourceFilepath']=objldb1["sourceFileName"]
                                detailsDict=uploadSourceDetail
                                
                                if(objldb1["isCleanedSource"])=="YES":                                 
                                    detailsDict["isCleanedSource"]="YES"
                                    detailsDict["isNormalSource"]="NO"
                                    
                                else:
                                    detailsDict["isNormalSource"]="YES"
                                    detailsDict["isCleanedSource"]="NO"
                                return detailsDict    
def isSourceValid(userName,userCategory,userDepartment,sourceDetail):
    source = sourceDetail['source']     
    print(source)  
    user= source['dataOwner']
    createdUser=source['createdUser']
    dataUser = [i ['name'] for i in source['dataUser']]
    settings=sourceDetail['settings']
    departmentList = settings['department']
    flag = False

    if user == userName:
        flag = True
    if createdUser == userName:
        flag = True             
    if len(userDepartment) > 0 :
        for userDepart in userDepartment:
            if userDepart in departmentList:
                flag =True
                break
    if len(userCategory) > 0 :
        for user in userCategory:
            if user in dataUser:
                flag =True
                break
              
    print(flag)        
    return flag

def getmaxuploadId(sourceUploadlist,sourceId,type):
    
    IDList=[0]
    if type=="normal":
        sourceUploadlist=[d for d in sourceUploadlist if d["isCleanedSource"]!="YES"]
    elif type=="clean":
        sourceUploadlist=[d for d in sourceUploadlist if d["isCleanedSource"]=="YES"]    
    
             
    for obj1 in sourceUploadlist:        
        IDList.append(obj1["uploadId"][(len(sourceId)+1):] )
    dli2 = [int(s) for s in IDList]
    return (str(max(dli2)))
       

def isprofileSourceValid(userName,userCategory,userDepartment,sourceDetail):
    source = sourceDetail      
    user= source['dataOwner']
    createdUser=source['createdUser']
    
    departmentList = source['department']
    flag = False

    if user == userName:
        flag = True
    if createdUser == userName:
        flag = True             
    if len(userDepartment) > 0 :
        for userDepart in userDepartment:
            if userDepart in departmentList:
                flag =True
                break
    print(sourceDetail)
    print(flag)
    return flag

@app.route('/api/getsourceDetails', methods=['POST'])
def getsourceDetails():
    analysisList=[]
    data={}

    content1 = request.get_json()
    sourceId = content1['sourceId']

    with open('db.json', 'r') as openfile:
        json_object = json.load(openfile)
    data = json_object

    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)

    dataldb = json_object
    LDBData = dataldb['LaunchAnalysis']

    with open('cleandb.json', 'r') as openfile:
        json_object = json.load(openfile)

    cleandb_data = json_object["SourceDetailsList"]

    with open('profiledb.json', 'r') as openfile:
        profiledb_json_object = json.load(openfile)
    sourcelist= [d for d in data['Analysis'] if (d["sourceId"])==sourceId]
    print(sourcelist)
    for obj in sourcelist:
        tempDict={}
        temprules=[]
        uploadSourceList=[]
        cleanedhistList = []
        recentsourceUpload={}
        tempDict['sourceId']= obj['sourceId']
        sourceId= obj['sourceId']
        tempDict['source']= obj['source']
        sourceData= obj['source']
        tempDict['reference']= obj['reference']
        tempDict['settings']=obj['settings']
        datedHistory=[]
        LDBDataList=[]
        LDBDataList=[d for d in LDBData if (d["sourceId"])==sourceId]
        for objldb in LDBDataList:
                dateList=[]
                dateLitnew=[]
                dateLitnew=(([((datetime.strptime(d['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")).date()) for d in objldb["uploads"] ]))                
                test_list = list(set(dateLitnew))
                for eachdate in test_list:                    
                    datelist= [d for d in objldb["uploads"] if (datetime.strptime(d["uploadDate"],"%Y-%m-%dT%H:%M:%S.%fZ")).date()==eachdate] 
                    #li= [d for d in datelist if (d["isCleanedSource"])=="YES"] 
                    dateuploadList={}
                    dateuploadList['uploadDate'] =eachdate
                    uploadLIst=[]
                    for objldb1 in datelist:
                            detailsDict={}                             
                            uploadSourceDetail= {}
                            uploadSourceDetail['uploadId'] =objldb1["uploadId"]
                            uploadSourceDetail['isMultiSource'] =objldb1["isMultiSource"]
                            if(objldb1["isMultiSource"]=="Yes"):
                                multiSourcePathObj= objldb1['multiSourcePath']
                                uploadSourceDetail['multiSourceKey'] =multiSourcePathObj["multiSourceKey"]
                            uploadSourceDetail['uploadDate'] =objldb1["uploadDate"]
                            uploadSourceDetail['uploadTime'] =objldb1["uploadTime"]
                            uploadSourceDetail['sourceFileName']=objldb1["sourceFileName"]
                            if(objldb1["sourceFileName"]!=""):
                                if "cleaned_data/" in objldb1["sourceFileName"]:
                                    x = objldb1["sourceFileName"].strip("cleaned_data/")
                                    uploadSourceDetail['sourceFileName']=x
                            uploadSourceDetail['sourceFilepath']=objldb1["sourceFileName"]
                            detailsDict=uploadSourceDetail
                            if(objldb1["isCleanedSource"])=="YES":                                 
                                detailsDict["isCleanedSource"]="YES"
                                detailsDict["isNormalSource"]="NO"
                            else:
                                detailsDict["isNormalSource"]="YES"
                                detailsDict["isCleanedSource"]="NO"
                            
                            uploadSourceList.append(uploadSourceDetail)
                            uploadLIst.append(detailsDict)
                            recentsourceUpload=uploadSourceDetail
                    dateuploadList['uploadDetails'] =uploadLIst
                    datedHistory.append(dateuploadList)
                for objcleandb in cleandb_data:
                    if objcleandb["sourceId"]==sourceId:
                        cleanedhistList.append(objcleandb)

        for obj1 in obj["rules"]:
            temprulesDict={}
            temprulesDict['sourceId']= obj['sourceId']
            temprulesDict["rulesetId"]=obj1["rulesetId"]
            temprulesDict["selectedColumns"]=    obj1["selectedColumns"]
            #temprulesDict["Referencepath"]=    obj1["Referencepath"]
            temprulesDict["refSelectedColumns"]=    obj1["refSelectedColumns"]
            temprulesDict['startDate'] = obj1['startDate']
            temprulesDict['endDate'] = obj1['endDate']
            temprulesDict["ruleset"]= obj1["ruleset"]
            temprulesDict["rulesetName"]= obj1["rulesetName"]
            temprulesDict["columns"]= sourceData["availableColumns"]

            startDate_obj =  datetime.strptime(obj1['startDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
            endDate_obj = datetime.strptime(obj1['endDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

            if startDate_obj.date() <= date.today() <= endDate_obj.date():
                temprulesDict["Rulestatus"]= "Active"
            elif startDate_obj.date() >= date.today():
                temprulesDict["Rulestatus"]= "Inactive"
            else:
                temprulesDict["Rulestatus"]= 'Expired'

            temprules.append(temprulesDict)
        tempDict['rules']=temprules
        tempDict["UploadsHistory"]= uploadSourceList
        tempDict["DatedUploadHistory"]=datedHistory
        tempDict["CleanedHistory"] = cleanedhistList
        tempDict["ProfiledBHistory"] = profiledb_json_object["SourceDetailsList"]
        tempDict['recentsourceUpload'] = recentsourceUpload
        analysisList.append(tempDict)
    data['Analysis'] = analysisList
    jsonString = json.dumps(data, default=str)
    return jsonString

@app.route('/api/getdetailbyDbId', methods=['POST'])
def getdetailbyDbId():
    
    data={}
    content = request.get_json()
    sourceId= content['sourceId']
    module = content['module']
    if module=="DQM":
        return getDQMDetailsbySourceid(sourceId=sourceId)
    elif module=="PROFILE":   
        return GetAllProfiledBdetailbyId(sourceId=sourceId)
    elif module=="CLEAN":
        return GetAllCleandBdetailbyId(sourceId=sourceId)
    else: 
        data['Analysis'] = 'no data'
        jsonString = json.dumps(data, default=str)
        return jsonString
def getDQMDetailsbySourceid(sourceId):
    analysisList=[]
    data={}
    with open('db.json', 'r') as openfile:
        json_object = json.load(openfile)
    data = json_object

    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)

    dataldb = json_object
    LDBData = dataldb['LaunchAnalysis']

    with open('cleandb.json', 'r') as openfile:
        json_object = json.load(openfile)

    cleandb_data = json_object["SourceDetailsList"]

    with open('profiledb.json', 'r') as openfile:
        profiledb_json_object = json.load(openfile)
    dqmsingleList=[]
    dqmsingleList=[d for d in data['Analysis'] if (d["sourceId"])==sourceId]
    for obj in dqmsingleList:
        tempDict={}
        temprules=[]
        uploadSourceList=[]
        cleanedhistList = []
        recentsourceUpload={}
        tempDict['sourceId']= obj['sourceId']
        sourceId= obj['sourceId']
        tempDict['source']= obj['source']
        sourceData= obj['source']
        tempDict['reference']= obj['reference']
        tempDict['settings']=obj['settings']
        datedHistory=[]
        LDBDataList=[]
        LDBDataList=[d for d in LDBData if (d["sourceId"])==sourceId]
        for objldb in LDBDataList:
                dateList=[]
                dateLitnew=[]
                dateLitnew=(([((datetime.strptime(d['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")).date()) for d in objldb["uploads"] ]))                
                test_list = list(set(dateLitnew))
                for eachdate in test_list:                    
                    datelist= [d for d in objldb["uploads"] if (datetime.strptime(d["uploadDate"],"%Y-%m-%dT%H:%M:%S.%fZ")).date()==eachdate] 
                    #li= [d for d in datelist if (d["isCleanedSource"])=="YES"] 
                    dateuploadList={}
                    dateuploadList['uploadDate'] =eachdate
                    uploadLIst=[]
                    for objldb1 in datelist:
                            detailsDict={}                             
                            uploadSourceDetail= {}
                            uploadSourceDetail['uploadId'] =objldb1["uploadId"]
                            uploadSourceDetail['isMultiSource'] =objldb1["isMultiSource"]
                            if(objldb1["isMultiSource"]=="Yes"):
                                multiSourcePathObj= objldb1['multiSourcePath']
                                uploadSourceDetail['multiSourceKey'] =multiSourcePathObj["multiSourceKey"]
                            uploadSourceDetail['uploadDate'] =objldb1["uploadDate"]
                            uploadSourceDetail['uploadTime'] =objldb1["uploadTime"]
                            uploadSourceDetail['sourceFileName']=objldb1["sourceFileName"]
                            if(objldb1["sourceFileName"]!=""):
                                if "cleaned_data/" in objldb1["sourceFileName"]:
                                    x = objldb1["sourceFileName"].strip("cleaned_data/")
                                    uploadSourceDetail['sourceFileName']=x
                            uploadSourceDetail['sourceFilepath']=objldb1["sourceFileName"]
                            detailsDict=uploadSourceDetail
                            if(objldb1["isCleanedSource"])=="YES":                                 
                                detailsDict["isCleanedSource"]="YES"
                                detailsDict["isNormalSource"]="NO"
                            else:
                                detailsDict["isNormalSource"]="YES"
                                detailsDict["isCleanedSource"]="NO"
                            
                            uploadSourceList.append(uploadSourceDetail)
                            uploadLIst.append(detailsDict)
                            recentsourceUpload=uploadSourceDetail
                    dateuploadList['uploadDetails'] =uploadLIst
                    datedHistory.append(dateuploadList)
                for objcleandb in cleandb_data:
                    if objcleandb["sourceId"]==sourceId:
                        cleanedhistList.append(objcleandb)

        for obj1 in obj["rules"]:
            temprulesDict={}
            temprulesDict['sourceId']= obj['sourceId']
            temprulesDict["rulesetId"]=obj1["rulesetId"]
            temprulesDict["selectedColumns"]=    obj1["selectedColumns"]
            #temprulesDict["Referencepath"]=    obj1["Referencepath"]
            temprulesDict["refSelectedColumns"]=    obj1["refSelectedColumns"]
            temprulesDict['startDate'] = obj1['startDate']
            temprulesDict['endDate'] = obj1['endDate']
            temprulesDict["ruleset"]= obj1["ruleset"]
            temprulesDict["rulesetName"]= obj1["rulesetName"]
            temprulesDict["columns"]= sourceData["availableColumns"]

            startDate_obj =  datetime.strptime(obj1['startDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
            endDate_obj = datetime.strptime(obj1['endDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

            if startDate_obj.date() <= date.today() <= endDate_obj.date():
                temprulesDict["Rulestatus"]= "Active"
            elif startDate_obj.date() >= date.today():
                temprulesDict["Rulestatus"]= "Inactive"
            else:
                temprulesDict["Rulestatus"]= 'Expired'

            temprules.append(temprulesDict)
        tempDict['rules']=temprules
        tempDict["UploadsHistory"]= uploadSourceList
        tempDict["DatedUploadHistory"]=datedHistory
        tempDict["CleanedHistory"] = cleanedhistList
        tempDict["ProfiledBHistory"] = profiledb_json_object["SourceDetailsList"]
        tempDict['recentsourceUpload'] = recentsourceUpload
        analysisList.append(tempDict)
    data['Analysis'] = analysisList
    jsonString = json.dumps(data, default=str)
    return jsonString


def GetAllProfiledBdetailbyId(sourceId):
    '''API to retrieve complete profiledb json log file'''
    data1 = json.load(open("profiledb.json","r"))
    profilesingleList=[]
    profilesingleList=[d for d in data1['SourceDetailsList'] if (d["sourceId"])==sourceId]
    data={}
    data['Analysis'] = profilesingleList
    jsonString = json.dumps(data, default=str)
    return jsonString


def GetAllCleandBdetailbyId(sourceId):
    '''API to retrieve complete cleandb json log file'''
    data1 = json.load(open("cleandb.json","r"))
    cleansingleList=[]
    cleansingleList=[d for d in data1['SourceDetailsList'] if (d["sourceId"])==sourceId]
    data={}
    data['Analysis'] = cleansingleList
    jsonString = json.dumps(data, default=str)
    return jsonString


def LaunchAnalysisthread(sourceId,rulesetId,KeyNameList,uploadId):
    print('starting long running task')
    rules=[]
    #keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file
    rulesObject={}
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis']
    AnalysisObj=sourceObj['source']

    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList']

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['exactuploadList']
    EntireSourceList=LaunchEntityRaw['exactSourceList']
    sourcepath= LaunchEntity['sourcePath']
    multiSourceKey=""
    if(LaunchEntity['isMultiSource']=='Yes'):
        multiSources= LaunchEntity['multiSourcePath']
        sourcepath=multiSources['sourcePath']
        multiSourceKey=multiSources['multiSourceKey']
    else:
        sourcepath= LaunchEntity['sourcePath']
        multiSourceKey=""
    AnalysisResultList= LaunchEntity['AnalysisResultList']
    launchMaxId= LaunchEntityRaw['launchMaxId']
    existingLaunchSourceList=[]
    for obj1 in EntireSourceList["uploads"]:
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= sourceId + 'L' + str(int(launchMaxId)+1)
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]
            ruleset= obj1["ruleset"]
    df= df_from_path(sourcepath)        
    # file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    # if(file_ext=='.csv'):
    #     df = pd.read_csv(sourcepath)
    # elif(file_ext=='.xls'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # elif(file_ext=='.xlsx'):
    #     df = pd.read_excel(sourcepath)
    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # else:
    #     df = pd.read_csv(sourcepath)

    df2 = pd.DataFrame(df, columns=cdecolumns)
    #df2 = df1.where(pd.notnull(df1), 'None')


    q = mp.Queue()
    jobs = []
    i=0

    for keyItem in KeyNameList:
        i=i+1
        launchNewId=sourceId + 'L' + str(int(launchMaxId)+i)
        p = mp.Process(target=LaunchAnalysisbyKeyNames, args=(df2,keyItem,launchNewId,ruleset,cdecolumns,ReferenceObj,q))
        jobs.append(p)
        p.start()
    #for p in jobs:
    #    p.join()
    #    print('joining results')
    print('checking results')

    results = [q.get() for j in jobs]

    #AnalysisResultList.append(results)
    LaunchEntity['AnalysisResultList']='R_'+(uploadId)
    print('saving main results')
    fResult={}
    fResult['AnalysisResultList']=results
    saveResultsets(fResult,'R_'+(uploadId))
    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity

    jsonString = json.dumps(data, default=str)

    json.dump(data, open("ldb.json","w"), default=str)
    return 'Completed'


def processSourcethread(sourceId,rulesetId,KeyNameList,uploadId,uploadDate,sourcedetails,settings):
    print('starting long running task')
    rules=[]
    #keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file
    rulesObject={}
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis']
    AnalysisObj=sourceObj['source']

    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList']

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))
    print(LaunchEntityRaw)
    LaunchEntity=LaunchEntityRaw['exactuploadList']
    EntireSourceList=LaunchEntityRaw['exactSourceList']
    sourcepath= LaunchEntity['sourcePath']
    multiSourceKey=""
    if(LaunchEntity['isMultiSource']=='Yes'):
        multiSources= LaunchEntity['multiSourcePath']
        sourcepath=multiSources['sourcePath']
        multiSourceKey=multiSources['multiSourceKey']
    else:
        sourcepath= LaunchEntity['sourcePath']
        multiSourceKey=""
    AnalysisResultList= LaunchEntity['AnalysisResultList']
    launchMaxId= LaunchEntityRaw['launchMaxId']
    existingLaunchSourceList=[]
    for obj1 in EntireSourceList["uploads"]:
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= uploadId + 'L' + str(int(launchMaxId)+1)
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]
            ruleset= obj1["ruleset"]
    if AnalysisObj["type"]== "oracle":
        if AnalysisObj["type"]== "oracle":
            typename= "oracle"
            connectionDetails=AnalysisObj["connectionDetails"]
            connContent=AnalysisObj["connectionDetails"]
            host=connContent['host']
            dbName = connContent['dbName']
            userName = connContent['username']
            port = connContent['port']
            password = connContent['password']
            table = connContent['sourceTableName']
            df = getdfFromTable(host,dbName,port,userName,password,table)
    else:
        print(sourcepath)
        df= df_from_path_uploadSource(sourcepath)
        

    df2 = pd.DataFrame(df, columns=cdecolumns)
    #df2 = df1.where(pd.notnull(df1), 'None')
    LaunchEntity['AnalysisResultList']='R_'+(uploadId)
    LaunchEntity['launchMaxId']=uploadId + 'L' + str(len(KeyNameList)+1)
    print('saving main results')
    # fResult={}
    # fResult['AnalysisResultList']=results
    # saveResultsets(fResult,'R_'+(uploadId))
    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity

    jsonString = json.dumps(data, default=str)

    json.dump(data, open("ldb.json","w"), default=str)

    q = mp.Queue()
    jobs = []
    i=0

    for keyItem in KeyNameList:
        i=i+1
        launchNewId=uploadId + 'L' + str(i)
        p = mp.Process(target=processSourceByKeyName, args=(df2, keyItem,launchNewId,ruleset,cdecolumns,ReferenceObj,uploadId,q))
        jobs.append(p)
        p.start()
    #for p in jobs:
    #    p.join()
    #    print('joining results')
    print('checking results')

    results = [q.get() for j in jobs]
    testResultList=[]
    testResultList=testResultList+(results)
    
    completeness=[]
    Accuracy=[]
    Uniqueness=[]
    Integrity=[]
    Validity=[]
    for finalResults in testResultList:
        print(finalResults)
        if 'Completeness' in finalResults:
            completeness.append(float(finalResults['Completeness']))
        if 'Accuracy' in finalResults:
            Accuracy.append(float(finalResults['Accuracy']))
        if 'Uniqueness' in finalResults:
            Uniqueness.append(float(finalResults['Uniqueness']))
        if 'Integrity' in finalResults:
            Integrity.append(float(finalResults['Integrity']))
        if 'Validity' in finalResults:
            Validity.append(float(finalResults['Validity']))


    DictFinal={}
    if len(completeness)>0:
        cPerc= sum(completeness)/len(completeness)
        DictFinal['Completeness'] = str(round(cPerc, 2))
    if len(Accuracy)>0:
        cPerc= sum(Accuracy)/len(Accuracy)
        DictFinal['Accuracy'] = str(round(cPerc, 2))
    if len(Uniqueness)>0:
        cPerc= sum(Uniqueness)/len(Uniqueness)
        DictFinal['Uniqueness'] = str(round(cPerc, 2))
    if len(Integrity)>0:
        cPerc= sum(Integrity)/len(Integrity)
        DictFinal['Integrity'] = str(round(cPerc, 2))
    if len(Validity)>0:
        cPerc= sum(Validity)/len(Validity)
        DictFinal['Validity'] = str(round(cPerc, 2))
    sourcedataName=AnalysisObj["sourceDataName"]
    sourcedataDesc=AnalysisObj["sourceDataDescription"]
    stype=AnalysisObj["type"]
    addAggResults(sourceId,sourcedetails,uploadId,uploadId,rulesetId,uploadDate,"0.00",DictFinal,sourcedataName,sourcedataDesc,stype,settings)


    userList=[]
    userMsgList=[]
    #for name in AnalysisObj["dataOwner"]:
    with open('userdb.json', 'r') as openfile:
            json_object_user = json.load(openfile)
    if AnalysisObj["dataOwner"]!="":
        userMsg={}
        userMsg=getUsersFromownername(uploadDate ,AnalysisObj["sourceDataName"],AnalysisObj["dataOwner"],json_object_user)
        
        userMsgList=userMsgList+(userMsg)
        
    if "dataUser" in AnalysisObj:
        userMsgList=userMsgList+getUsersFromCategory(json_object_user,AnalysisObj["dataUser"],DictFinal,uploadDate,AnalysisObj["sourceDataName"],AnalysisObj["dataOwner"])
        
    if "dataProcessingOwner" in AnalysisObj:
        userMsgList=userMsgList+getUsersFromCategory(json_object_user,AnalysisObj["dataProcessingOwner"],DictFinal,uploadDate,AnalysisObj["sourceDataName"],AnalysisObj["dataOwner"])
       
    if "dataSteward" in AnalysisObj:
        message="The file is uploaded for "+ uploadDate +" and the results are available in DashBoard for the source Named "+AnalysisObj["sourceDataName"]
        userMsgList=userMsgList+getUserNamesFromCategory(json_object_user,AnalysisObj["dataSteward"],message)
        



    receiver_email = "suji.arumugam@gmail.com"
    # message="The file is uploaded for "+ uploadDate +" and the results are available in DashBoard for the source Named "+AnalysisObj["sourceDataName"]
    # print(userMsgList)
    addNotificationList(userMsgList ,"Active","UnRead")


    sendMailtoUserList(userMsgList)
    return 'Completed'


def sendMailtoUserList(userMsgList):
    
    checkList=[]
    for usermsg in [dict(t) for t in {tuple(d.items()) for d in userMsgList}]:
        sendMailtoUser(usermsg["msg"], usermsg["email"],usermsg["Name"])
    return "true"
    


def getUsersFromCategory_old(category)   :
    with open('userCategory.json', 'r') as openfile:
            json_object = json.load(openfile)
    categoryDict={}
    data=json_object
    userCategoryList=[]

    for obj in data["UserCategory"]:
        for categoryName in category:
            if (obj["category"]==categoryName):
                userCategoryList=userCategoryList+ obj["users"]
    return userCategoryList

def getUsersFromCategory(json_object,category,aggFinalResults,uploadDate,sourceName,dataowner)   :
    userMsgList=[]
    userCategoryList=[]
    data={}
    owner=""
    
    for name in dataowner:
        owner=name
    data=json_object
    for cat in category:
        for obj in data['UserList']:
            if( cat['name'] in obj["userCategory"]):
                if (checkToleranceLimit(aggFinalResults,cat['tolerance'])):
                    userCategoryList.append(obj["userName"])
                    userMsg={}
                    userMsg["user"]=obj["userName"]
                    message="The file is uploaded for "+ uploadDate +" and the results are available in DashBoard for the source Named "+sourceName
                    userMsg["msg"]= message
                    userMsg["email"]= obj['email']
                    userMsg["Name"]=obj['name']
                    userMsgList.append(userMsg)
                else:
                    userCategoryList.append(owner)
                    userMsg={}
                    userMsg["user"]=owner
                    message="The file which is uploaded for "+ uploadDate +" and corresponding to the source Named "+sourceName +"doesn't meet with the tolerance of the usercategory " + cat['name']
                    userMsg["msg"]= message
                    userMsg["email"]= obj['email']
                    userMsg["Name"]=obj['name']
                    userMsgList.append(userMsg)
    return userMsgList

def getUsersFromownername(uploadDate,sourceName,dataowner,json_object)   :
    userMsgList=[]
    userCategoryList=[]
    data={}
    owner=""
    print(dataowner)
    print("dtsttsdsyhdfk")
    for name in dataowner:
        owner=name
    data=json_object
    for obj in data['UserList']:
            if( name in obj["name"]):
                    userMsg={}
                    userMsg["user"]=obj["userName"]
                    message="The file is uploaded for "+ uploadDate +" and the results are available in DashBoard for the source Named "+sourceName
                    userMsg["msg"]= message
                    userMsg["email"]= obj['email']
                    userMsg["Name"]=obj['name']
                    userMsgList.append(userMsg)
    return userMsgList


def getUserNamesFromCategory(json_object,category,message)   :
    userMsgList=[]
    userCategoryList=[]
    data={}
    owner=""
    
    data=json_object
    # datalist=[d for d in data['UserList'] if d["userCategory"] in category]
    for cat in category:
        for obj in data['UserList']:
            if( cat in obj["userCategory"]):
                    userMsg={}
                    userMsg["user"]=obj['userName']
                    userMsg["msg"]= message
                    userMsg["email"]= obj['email']
                    userMsg["Name"]=obj['name']
                    userMsgList.append(userMsg)
    return userMsgList

def checkToleranceLimit(actual,tolerance):
    flag=True
    if 'Completeness' in actual:
            if float(actual['Completeness'])>=float(tolerance['Completeness']):
                flag=True
            else:
                return False
    if 'Accuracy' in actual:
        if float(actual['Accuracy'])>=float(tolerance['Accuracy']):
                flag=True
        else:
                return False
    if 'Uniqueness' in actual:
        if float(actual['Uniqueness'])>=float(tolerance['Uniqueness']):
                flag=True
        else:
                return False
    if 'Integrity' in actual:
        if float(actual['Integrity'])>=float(tolerance['Integrity']):
                flag=True
        else:
                return False
    if 'Validity' in actual:
        if float(actual['Validity'])>=float(tolerance['Validity']):
                flag=True
        else:
                return False
    return True


def processSourceByKeyName(df2,KeyName,launchNewId,ruleset,cdecolumns,ReferenceObj,uploadId,q):
    print('starting long running task')
    rules=[]
    keyv=KeyName
    rulesObject={}
    resultsetDictlist=[]
    dfs= df2.groupby([KeyName])
    DataList=[]
    DataList=[d["DataType"] for d in ruleset if (d["column"])==keyv]
    frametbpcolnm= df2.copy(deep=True)
    isOutlier= checkoutlierforkeyname(DataList,frametbpcolnm,ruleset,keyv)
    dtypevalue=DataList[0]
    print("testing value")
    completeness=[]
    Accuracy=[]
    Uniqueness=[]
    Integrity=[]
    Validity=[]

    completenessbycol=[]
    Accuracybycol=[]
    Uniquenessbycol=[]
    Integritybycol=[]
    Validitybycol=[]
    collistdetails=[]
    count=0
    try:
        for KeyName, frame in dfs:
            resultsetDictlist=[]
            Dict = {}
            Dict[keyv] =KeyName
            print("1111")
            if isOutlier=='true':
                print("1112")
                Dict['isOutlier'] =checkoutlierforkeyvalue(dtypevalue,KeyName)
                print("1113")
            else:
                Dict['isOutlier'] = 'false'
                print("1114")
            Dictconstraints = {}
            '''s1=(len(frame))

            s2= frame.isnull().sum().sum()
            v2=(s2/s1)*100
            v1=100-round(v2,2)'''
            print('KeyName')
            print(KeyName)
            Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)
            #rulesList.append(Dict)
            nan_values = frame.isna()
            nan_columns = nan_values.any()
            columns_with_nan = frame.columns[nan_columns].tolist()
            nulls_list=[]

            for k in columns_with_nan:
                DNull = {}
                DNull['column'] =k
                DNull['nullcount'] =str(frame[k].isnull().sum())
                val1= frame[frame[k].isnull()]
                resDf = val1.where(pd.notnull(val1), 'None')
                resDf.index = resDf.index + 2
                data_dict = resDf.to_dict('index')
                count=count+1
                resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                DNull['outlier']=resultId
                resultsetdict={}
                resultsetdict['resultset'] = resultId
                resultsetdict['results'] = data_dict
                #saveResultsets(data_dict,resultId)
                resultsetDictlist.append(resultsetdict)
                nulls_list.append(DNull)
                
            Dictconstraints['details']=nulls_list
            completeness.append(float(Dictconstraints['value']))
            #completenessbycol.append(collistdetails)
           
            
            Dict['completness'] = Dictconstraints
            
            dupCount=0
            duptotalCount=0
            dupPerc=0
            colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
            farmenew= frame#[colList]

            dupPerc= 100 -((farmenew.duplicated().mean())*100)

            nulls_list=[]
            DNull = {}
            DNull['column'] =str(colList)
            DNull['outLiers'] = str(farmenew.duplicated().sum())
            val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
            resDfa1 = val.where(pd.notnull(val), 'None')

            data_dict = resDfa1.to_dict('index')


            count=count+1
            resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
            DNull['outlier']=resultId
            resultsetdict1={}
            resultsetdict1['resultset'] = resultId
            resultsetdict1['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict1)
            nulls_list.append(DNull)
            DictUnconstraints = {}
            DictUnconstraints['value'] = str(round(dupPerc, 2))
            if farmenew.duplicated().sum()>0:
                DictUnconstraints['details']=nulls_list
            else:
                DictUnconstraints['details']=[]
            Dict['Uniqueness'] = DictUnconstraints
            Uniqueness.append(float(DictUnconstraints['value']))
            value=[100]
            Accvalue=[]
            valAccValue=[]
            isAccuracy= False
            isintegrity=False
            vnulls_list=[]
            AccNulls_list=[]
            framenew = frame.where(pd.notnull(frame), 0)
            #frame = frame.where(pd.notnull(frame), 'None')
            IntegValue=[]
            IntegNulls_list=[]
            #frame = frame.dropna()
            frametbp= frame.copy(deep=True)

            for r in ruleset:
                frametbpcol= frametbp.copy(deep=True)

                col=r['column']
                row_idx = frametbpcol[frametbpcol[col].isnull()].index.to_list()
                frametbpcol.drop(row_idx, axis = 0, inplace = True)
                frametbpcolnew= frametbpcol.copy(deep=True)
                framenew= frametbpcolnew.copy(deep=True)
                for ru in r['rules']:
                    if ru['rule']=='DataType':
                        if ru['value']=='alphabets' or ru['value']=='Alphanumeric' or ru['value']=='Text' :

                            cntalnum=frametbpcol[col].str.isalnum().sum()
                            cnttotal=frametbpcol[col].count()
                            if(cnttotal!=0):
                                percen=((cntalnum)/(cnttotal))*100
                                value.append( percen)
                            if(cnttotal!=cntalnum):
                                DNull = {}
                                DNull['column'] =col
                                DNull['ruleType']='DataType'
                                DNull['rulevalue']='alphabets'
                                DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                                r3= frametbpcol[~frametbpcol[col].str.isalnum()]
                                count=count+1
                                resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                                DNull['outlier']=resultId
                                #saveResultsets(r3.to_dict(),resultId)
                                resDfa2 = r3.where(pd.notnull(r3), 'None')
                                resDfa2.index = resDfa2.index + 2
                                data_dict = resDfa2.to_dict('index')

                                resultsetdict2={}
                                resultsetdict2['resultset'] = resultId
                                resultsetdict2['results'] = data_dict
                                #saveResultsets(data_dict,resultId)
                                resultsetDictlist.append(resultsetdict2)

                                vnulls_list.append(DNull)
                        elif ru['value']=='Numeric':
                            cnttotal=frametbpcol[col].sum()
                            frametbpcol[col]=frametbpcol[col].astype(str)
                            frametbpcol[col]=frametbpcol[col].str.replace(".", "",regex=False)
                            cntalnum=frametbpcol[col].str.isdigit().sum()
                            cnttotal=frametbpcol[col].count()
                            if(cnttotal!=0):
                                percen=((cntalnum)/(cnttotal))*100
                                value.append(percen)
                            if(cnttotal!=cntalnum):
                                DNull = {}
                                DNull['column'] =col
                                DNull['ruleType']='DataType'
                                DNull['rulevalue']='Numeric'
                                DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                                r4= frametbpcol[~frametbpcol[col].str.isdigit()]
                                count=count+1
                                resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                                DNull['outlier']=resultId
                                #saveResultsets(r4.to_dict(),resultId)

                                resDfa3 = r4.where(pd.notnull(r4), 'None')
                                resDfa3.index = resDfa3.index + 2
                                data_dict = resDfa3.to_dict('index')

                                resultsetdict3={}
                                resultsetdict3['resultset'] = resultId
                                resultsetdict3['results'] = data_dict
                                #saveResultsets(data_dict,resultId)
                                resultsetDictlist.append(resultsetdict3)
                                vnulls_list.append(DNull)

                    elif ru['rule']=='Length':
                        if (int(float(ru['value']))==frametbpcol[col].astype(str).str.len().min()) and  (int(float(ru['value']))==frametbpcol[col].astype(str).str.len().max()):
                            value.append(100)
                        else:
                            cnttotal=frametbpcol[col].count()
                            if(cnttotal!=0):
                                value.append(100-((((frametbpcol[col].astype(str).str.len() !=int(float(ru['value']))).sum())/(len(frametbpcol[col])))*100) )
                                DNull = {}
                                DNull['column'] =col
                                DNull['ruleType']='Length'
                                DNull['rulevalue']=ru['value']
                                DNull['ruleMismatchcount'] = str((frametbpcol[col].astype(str).str.len() !=int(float(ru['value']))).sum())
                                r5= frametbpcol[(frametbpcol[col].astype(str).str.len() !=int(float(ru['value'])))]
                                count=count+1
                                resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                                DNull['outlier']=resultId
                                #saveResultsets(r5.to_dict(),resultId)

                                resDfa5 = r5.where(pd.notnull(r5), 'None')
                                resDfa5.index = resDfa5.index + 2
                                data_dict = resDfa5.to_dict('index')

                                resultsetdict5={}
                                resultsetdict5['resultset'] = resultId
                                resultsetdict5['results'] = data_dict
                                #saveResultsets(data_dict,resultId)
                                resultsetDictlist.append(resultsetdict5)

                                vnulls_list.append(DNull)

                    elif ru['rule']=='ReferenceCDE':
                        isintegrity=True
                        colname= ru['value'].split('-', 2)[2]
                        referenceIdRule = ru['value'].split('-', 2)[1]
                        referepath=""
                        for referobj in ReferenceObj:
                            if referobj['referenceId']==referenceIdRule:
                                referepath=referobj['referencePath']
                                referenceDataName=referobj['referenceDataName']
                                ref_data_type=referobj['ref_data_type']

                        refpath=referepath
                        if ref_data_type == "MongoDB_RefData":
                            dfref= get_df_RefSQLlite(referenceDataName,referenceDataName)
                        else:
                            file_ext= os.path.splitext(os.path.basename(refpath))[1]
                            print(file_ext)
                            if(file_ext=='.csv'):
                                dfref = pd.read_csv(refpath)
                            elif(file_ext=='.xls'):
                                dfref = pd.read_excel(refpath)
                                csv_data = dfref.to_csv(index=None)
                                dfref = pd.read_csv(StringIO(csv_data))
                            elif(file_ext=='.xlsx'):
                                dfref = pd.read_excel(refpath)
                                csv_data = dfref.to_csv(index=None)
                                dfref = pd.read_csv(StringIO(csv_data))
                            else:
                                dfref = pd.read_csv(refpath)
                        refCount=0
                        reftotalCount=0
                        refPerc=0
                        refCount=(frametbpcol[col].isin(dfref[colname])).sum()
                        reftotalCount=len(frametbpcol[col])
                        refPerc= (refCount / reftotalCount)*100
                        nulls_list=[]
                        if (reftotalCount- refCount)>0:

                            DNull = {}
                            DNull['column'] =col
                            DNull['rule'] =str(ru['rule'])+" " +str(ru['value'])
                            DNull['outLiers'] = str(reftotalCount- refCount)
                            r6= frametbpcol[~(frametbpcol[col].isin(dfref[colname]))]
                            count=count+1
                            resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r6.to_dict(),resultId)
                            resDfa6 = r6.where(pd.notnull(r6), 'None')
                            resDfa6.index = resDfa6.index + 2
                            data_dict = resDfa6.to_dict('index')

                            resultsetdict6={}
                            resultsetdict6['resultset'] = resultId
                            resultsetdict6['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict6)
                            IntegValue.append(refPerc)
                            IntegNulls_list.append(DNull)
                            #nulls_list.append(DNull)
                    elif ru['rule'] =='Formula':
                        rulename=""
                        isAccuracy=True
                        formula= ru['value']
                        ifFlag=false
                        formulaText=""
                        for valcol in formula:
                            formulaText=valcol['formulaText'] 
                            if valcol['logic']=="If":
                                ifFlag=true                                
                                break
                        if ifFlag==true:
                            #framenew= framenew.dropna()

                            formula= ru['value']
                            querystring =GetQueryStringifnew(formula)
                            invertedQString=GetinvertQueryifString(formula)
                            framecopy= framenew.copy(deep=True)
                            print(querystring)
                            print(invertedQString)
                            print(col)
                            print('conditional vala main loop')

                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                print('actual complie with formule: ')
                                print( num_rows)
                                idf2= framenew.query(invertedQString)
                                x = pd.concat([df2, idf2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                print(r7.head())
                                num_rows= len(framecopy[col])-len(r7)
                                print('actual count with formule: ')
                                print( num_rows)
                                #print(len(df3))
                                #print((df3.index).tolist())
                            rulename=querystring
                        else:
                            querystring= formulaText#ru['formulaText']
                            formula= ru['value']
                            #querystring =GetQueryStringnew(formula)
                            #invertedQString=GetinvertQueryString(formula)
                            invertedQString=querystring
                            framecopy= framenew.copy(deep=True)
                            print(querystring)
                            print(invertedQString)
                            print(col)
                            print('conditional vala else loop')
                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                print('actual complie with formule: ')
                                print( num_rows)
                                
                                x = pd.concat([framenew, df2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                print('errror here')
                                print(len(r7))
                                print(len(framenew[col]))
                                num_rows= len(framenew[col])-len(r7)
                                print('actual count not equla to formule: ')
                                print( num_rows)
                                #print(len(df3))
                                #print((df3.index).tolist())
                            rulename=querystring

                           
                        refCount=0
                        reftotalCount=0
                        refPerc=0
                        refCount=num_rows
                        #r7.index = r7.index + 2
                        reftotalCount=len(framecopy[col])
                        if reftotalCount!=0:
                            refPerc= (refCount / reftotalCount)*100
                            Accvalue.append(refPerc)
                        nulls_list=[]
                        if(reftotalCount- refCount>0):
                            DNull = {}
                            DNull['column'] =col
                            DNull['rule'] = rulename
                            DNull['outLiers'] = str(reftotalCount- refCount)


                            count=count+1
                            resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                            DNull['outlier']=resultId
                            r7 = r7.where(pd.notnull(r7), 'None')
                            r7.index = r7.index + 2
                            data_dict = r7.to_dict('index')

                            resultsetdict7={}
                            resultsetdict7['resultset'] = resultId
                            resultsetdict7['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict7)
                            AccNulls_list.append(DNull)

                    elif ru['rule'] =='Value':
                        ComValue= ru['value']
                        #framenew = framenew.dropna()
                        oper=ru['operator']
                        isAccuracy=True
                        framecopy1= framenew.copy(deep=True)
                        if oper=='equalto' or oper=='Shouldbe':

                            if(ComValue.isnumeric()):

                                framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                elnum_rows = len(framenew[framenew[col].astype('float')==float(ComValue)])
                                eloulierIndex=framenew.index[framenew[col].astype('float')!=float(ComValue)].tolist()
                                V7=framecopy1.loc[eloulierIndex]
                            else:

                                framenew = framenew.replace(np.nan, "None", regex=True)
                                elnum_rows = len(framenew[(framenew[col].astype(str))==(ComValue)])
                                eloulierIndex=framenew.index[(framenew[col].astype(str))!=(ComValue)].tolist()
                                V7=framecopy1.loc[eloulierIndex]
                        elif oper=='greaterthanequalto':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')>=float(ComValue)])
                            eloulierIndex=framenew.index[framenew[col].astype('float')<float(ComValue)].tolist()

                            V7=framecopy1.loc[eloulierIndex]
                        elif oper=='lessthanequalto':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')<=float(ComValue)])
                            eloulierIndex=framenew.index[framenew[col].astype('float')>float(ComValue)].tolist()
                            V7=framecopy1.loc[eloulierIndex]
                        elif oper=='greaterthan':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')>float(ComValue)])
                            eloulierIndex=framenew.index[framenew[col].astype('float')<=float(ComValue)].tolist()

                            V7=framecopy1.loc[eloulierIndex]
                        elif oper=='lessthan':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')<float(ComValue)])
                            eloulierIndex=framenew.index[framenew[col].astype('float')>=float(ComValue)].tolist()

                            V7=framecopy1.loc[eloulierIndex]
                        if len(eloulierIndex)>0:
                            DvNull = {}
                            DvNull['column'] =col
                            DvNull['rule'] =str(ru['rule'])+ " " +str(ru['operator']) + " " +str(ru['value'])
                            DvNull['ruleType']='Value'
                            DvNull['rulevalue']=ru['value']
                            DvNull['ruleMismatchcount'] = str(len(eloulierIndex))
                            count=count+1
                            resultId= (launchNewId)+str(KeyName) +'RS'+str(count)

                            DvNull['outlier']=resultId
                            #saveResultsets(r5.to_dict(),resultId)

                            resDfv7 = V7.where(pd.notnull(V7), 'None')
                            resDfv7.index = resDfv7.index + 2
                            data_dict = resDfv7.to_dict('index')

                            resultsetdictv7={}
                            resultsetdictv7['resultset'] = resultId
                            resultsetdictv7['results'] = data_dict

                            resultsetDictlist.append(resultsetdictv7)
                            AccNulls_list.append(DvNull)
                        refCount1=elnum_rows

                        reftotalCount1=len(framecopy1[col])
                        refPerc1= (refCount1 / reftotalCount1)*100
                        valAccValue.append(refPerc1)


            res= sum(value)/len(value)
            Dictvconstraints = {}
            Dictvconstraints['value'] = (round(res, 2))
            Dictvconstraints['details']=vnulls_list
            Dict['Validity'] = Dictvconstraints
            Validity.append(float(Dictvconstraints['value']))
            Aresperc=0

            if isintegrity== True:
                print(IntegValue)
                DictInconstraints = {}
                if len(IntegValue)>0:
                    resInt= sum(IntegValue)/len(IntegValue)
                else:
                    resInt=100

                DictInconstraints['value'] = str(round(resInt, 2))
                DictInconstraints['details']=IntegNulls_list
                Dict['Integrity'] = DictInconstraints
                Integrity.append(float(DictInconstraints['value']))

            if isAccuracy== True:
                print('inside accuracy loop')
                print(Accvalue)
                DictInconstraints = {}
                Aresperc=100
                if len(Accvalue)>0:
                    resAcc= sum(Accvalue)/len(Accvalue)
                    Aresperc=resAcc

                if len(valAccValue)>0:
                    resAcc1= sum(valAccValue)/len(valAccValue)
                    Aresperc=resAcc1
                if (len(Accvalue)>0) and (len(valAccValue)>0):
                    Aresperc=(resAcc+resAcc1)/2


                DictInconstraints['value'] = str(round(Aresperc, 2))
                DictInconstraints['details']=AccNulls_list
                Dict['Accuracy'] = DictInconstraints
                Accuracy.append(float(DictInconstraints['value']))
                print(Dict)
            rules.append(Dict)
            print('calling the saveresultset to s3')

            saveResultsets(resultsetDictlist,(launchNewId)+str(KeyName))
        # print('calling the saveresultset to s3')

        # saveResultsets(resultsetDictlist,(launchNewId))

        resultobject={}
        resultobject['launchId']=launchNewId
        resultobject['keyName']=keyv
        resultobject['results']=rules
        resultobject['isException']='No'
        DictFinal={}
        if len(completeness)>0:
            cPerc= sum(completeness)/len(completeness)
            DictFinal['Completeness'] = str(round(cPerc, 2))
        if len(Accuracy)>0:
            cPerc= sum(Accuracy)/len(Accuracy)
            DictFinal['Accuracy'] = str(round(cPerc, 2))
        if len(Uniqueness)>0:
            cPerc= sum(Uniqueness)/len(Uniqueness)
            DictFinal['Uniqueness'] = str(round(cPerc, 2))
        if len(Integrity)>0:
            cPerc= sum(Integrity)/len(Integrity)
            DictFinal['Integrity'] = str(round(cPerc, 2))
        if len(Validity)>0:
            cPerc= sum(Validity)/len(Validity)
            DictFinal['Validity'] = str(round(cPerc, 2))
        resultobject['aggResults']=DictFinal
        resultobject['isOutlier']=isOutlier
        fResult={}
        fResult['AnalysisResultList']=resultobject
        print('saving main result set')
        saveResultsets(fResult,'R_'+(uploadId)+ keyv)
        print('saving main result set as' + 'R_'+(uploadId)+ keyv)
        print(collistdetails)

        q.put(DictFinal)
    except Exception as e:
        resultobject={}
        resultobject['launchId']=launchNewId
        resultobject['keyName']=keyv
        resultobject['results']=rules
        resultobject['aggResults']={}
        resultobject['isException']='Yes'
        resultobject['exception']=  str(e)
        print(str(e))
        fResult={}
        fResult['AnalysisResultList']=resultobject
        saveResultsets(fResult,'R_'+(uploadId)+ keyv)
        q.put({})




def checkoutlierforkeyname(DataList,frametbpcolnm,ruleset,keyname):
    
    col=keyname
    row_idx = frametbpcolnm[frametbpcolnm[col].isnull()].index.to_list()
    frametbpcolnm.drop(row_idx, axis = 0, inplace = True)
    
    for ru in DataList: 
        if ru=='Alphanumeric':
            cntalnum=frametbpcolnm[col].str.isalnum().sum()
            cnttotal=frametbpcolnm[col].count()
            if(cnttotal!=cntalnum):
                return 'true'
            else:
                return 'false'
        elif ru=='Text':
            cntalnum=frametbpcolnm[col].str.isalnum().sum()
            cnttotal=frametbpcolnm[col].count()
            if(cnttotal!=cntalnum):
                return 'true'
            else:
                return 'false'
        elif ru=='Numeric':
            cnttotal=frametbpcolnm[col].sum()
            frametbpcolnm[col]=frametbpcolnm[col].astype(str)
            frametbpcolnm[col]=frametbpcolnm[col].str.replace(".", "",regex=False)
            cntalnum=frametbpcolnm[col].str.isdigit().sum()
            cnttotal=frametbpcolnm[col].count()
            if(cnttotal!=cntalnum): 
                return 'true'
            else:
                return 'false'
        else:
            return 'false'
def checkoutlierforkeyvalue(dtype,keyvalue):
        if dtype=='Alphanumeric':
            if(str(keyvalue).isalnum()):
                return 'false'
            else:
                return 'true'
        elif dtype=='Text':            
            if(str(keyvalue).isalpha() and str(keyvalue).isalnum()):
                return 'false'
            else:
                return 'true'
        elif dtype=='Numeric':
            if(str(keyvalue).isnumeric()): 
                return 'false'
            else:
                return 'true'
        else:
            return 'true'
                                                        
@app.route('/api/uploadSourcewithoutOracle', methods=['POST'])
def uploadSourcewithoutOracle():
    uploaded_files = request.files.getlist("file[]")
    content =json.loads(request.form.get('data'))
    sourceId= content['sourceId']
    rulesetId= content['rulesetId']
    isMultiSource= content['isMultiSource']
    multiSourceKey= content['multiSourceKey']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['sourceObj']

    settings= content['settings']
    sourceFilename = sourcedetails['sourceFileName']
    sourceCatColumns = sourcedetails['categorialColumns']
    sourcePath=''
    #keyNametoLaunch= sourceCatColumns[0]
    newuploadId=sourceId+'U'+ str(int(getUploadMaxId(sourceId))+1)
    sourcename = os.path.splitext(sourceFilename)[0]
    print(sourcename)
    sourceFilename=newuploadId + sourcename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB(sourceId))

    EntireSourceObj= LaunchEntityRaw['EntireSourceObj']
    EntireSourceList= LaunchEntityRaw['EntireSourceList']
    uploadsObject= EntireSourceObj['uploads']

    print(uploadReason)
    if(uploadReason==''):
        if(isMultiSource=="Yes"):
            if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                        multisrObj= item['multiSourcePath']
                        if multisrObj['multiSourceKey']==multiSourceKey:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file for this multisource key is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
        else:
                if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)

    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
                print(sourceFilename)
                file_ext = os.path.splitext(filename)[1]
                sourceFilename=sourceFilename+file_ext
                print(file_ext)
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    dli=[]
                    sourcePath=sourceFilename
                    file.save(sourcePath)
                    df= df_from_path(sourcePath)
                    # if(file_ext=='.csv'):
                    #     df = pd.read_csv(sourcePath)
                    # elif(file_ext=='.xls'):
                    #     df = pd.read_excel(sourcePath)
                    #     csv_data = df.to_csv(index=None)
                    #     df = pd.read_csv(StringIO(csv_data))
                    # elif(file_ext=='.xlsx'):
                    #     df = pd.read_excel(sourcePath)
                    #     csv_data = df.to_csv(index=None)
                    #     df = pd.read_csv(StringIO(csv_data))
                    # else:
                    #     df = pd.read_csv(sourcePath)
                    dli= list(df.columns.values)

                    sourceavailableColumns=sourcedetails['availableColumns']


                    if dli != sourceavailableColumns:
                        content['errorMsg']= 'The file headers does not match with the configured source'
                        content['errorflag']= 'True'
                        content['errorCode']= '103'
                        return json.dumps(content)
    resultContent={}

    resultContent['launchId']=newuploadId
    resultContent['uploadId']=newuploadId
    resultContent['rulesetId']=rulesetId
    resultContent['sourceFileName']=sourcePath
    resultContent['sourcePath']=sourcePath
    resultContent['uploadDate']=uploadDate
    resultContent['uploadTime']=uploadTime
    resultContent['isexpecteddate']='Yes'
    resultContent['reference']=[]
    resultContent['launchAnalysis']={}
    resultList=[]

    resultContent['AnalysisResultList']=''
    resultContent['isMultiSource']=isMultiSource
    resultContent['multiSource']=[]
    #multiSourcePaths=[]
    if(isMultiSource=="Yes"):
        mpath={}
        mpath['sourcePath']=  sourcePath
        mpath['multiSourceKey']=multiSourceKey
        #multiSourcePaths.append(mpath)
        resultContent['multiSourcePath']=mpath
    content['uploadId']=newuploadId
    content['sourceFilename']=sourceFilename
    uploadsObject.append(resultContent)
    EntireSourceObj['uploads']=uploadsObject
    EntireSourceObj['sourceId']=sourceId
    EntireSourceList.append(EntireSourceObj)
    data={}
    data['LaunchAnalysis']=EntireSourceList
    jsonString= json.dumps(data, default=str)
    json.dump(data, open("ldb.json","w"), default=str)
    content1={}
    content1['errorMsg']= ''
    content1['errorflag']= 'False'
    content1['responseObj']= jsonString

    tempDict={}
    tempDict['sourceId']= sourceId
    tempDict['uploadId'] =newuploadId
    tempDict['source']= sourcedetails
    uploadSourceDetail= {}
    uploadSourceDetail['uploadId'] =newuploadId
    uploadSourceDetail['isMultiSource'] =isMultiSource
    uploadSourceDetail['uploadDate'] =uploadDate
    uploadSourceDetail['uploadTime'] =uploadTime
    uploadSourceDetail['sourceFileName']=sourceFilename
    tempDict['rules']=[]
    tempDict["UploadsHistory"]= []
    tempDict['recentsourceUpload'] = uploadSourceDetail


    inputcontent1={}
    inputcontent1['sourceId']=sourceId
    inputcontent1['uploadId']=newuploadId
    inputcontent1['rulesetId']=rulesetId
    inputcontent1['keyname']=sourceCatColumns




    def long_running_task(**kwargs):
        inputcontent = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", inputcontent)
        rules=[]
        time.sleep(1)
        sourceId = inputcontent['sourceId']
        rulesetId = inputcontent['rulesetId']
        KeyName = inputcontent['keyname']
        uploadId=inputcontent['uploadId']
        #LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId)
        processSourcethread(sourceId,rulesetId,KeyName,uploadId,uploadDate)
        print('Completed the main thread function')
    tempDict['stuats'] ='started'
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': inputcontent1})
    thread.start()

    return json.dumps(tempDict, default=str)



def LaunchAnalysisbyKeyNames(df2,KeyName,launchNewId,ruleset,cdecolumns,ReferenceObj,q):
    print('starting long running task')
    rules=[]
    keyv=KeyName
    print(KeyName)

    print(launchNewId)
    rulesObject={}
    resultsetDictlist=[]
    dfs= df2.groupby([KeyName])
    completeness=[]
    Accuracy=[]
    Uniqueness=[]
    Integrity=[]
    Validity=[]
    count=0
    try:
        for KeyName, frame in dfs:
            Dict = {}
            Dict[keyv] =KeyName
            Dictconstraints = {}
            '''s1=(len(frame))

            s2= frame.isnull().sum().sum()
            v2=(s2/s1)*100
            v1=100-round(v2,2)'''
            Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)
            #rulesList.append(Dict)
            nan_values = frame.isna()
            nan_columns = nan_values.any()
            columns_with_nan = frame.columns[nan_columns].tolist()
            nulls_list=[]
            for k in columns_with_nan:
                DNull = {}
                DNull['column'] =k
                DNull['nullcount'] =str(frame[k].isnull().sum())
                val1= frame[frame[k].isnull()]
                resDf = val1.where(pd.notnull(val1), 'None')
                resDf.index = resDf.index + 2
                data_dict = resDf.to_dict('index')
                count=count+1
                resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                DNull['outlier']=resultId
                resultsetdict={}
                resultsetdict['resultset'] = resultId
                resultsetdict['results'] = data_dict
                #saveResultsets(data_dict,resultId)
                resultsetDictlist.append(resultsetdict)
                nulls_list.append(DNull)
            Dictconstraints['details']=nulls_list
            completeness.append(float(Dictconstraints['value']))

            Dict['completness'] = Dictconstraints

            dupCount=0
            duptotalCount=0
            dupPerc=0
            colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
            farmenew= frame#[colList]

            dupPerc= 100 -((farmenew.duplicated().mean())*100)

            nulls_list=[]
            DNull = {}
            DNull['column'] =str(colList)
            DNull['outLiers'] = str(farmenew.duplicated().sum())
            val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
            resDfa1 = val.where(pd.notnull(val), 'None')

            data_dict = resDfa1.to_dict('index')


            count=count+1
            resultId= launchNewId +'RS'+str(count)
            DNull['outlier']=resultId
            resultsetdict1={}
            resultsetdict1['resultset'] = resultId
            resultsetdict1['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict1)
            nulls_list.append(DNull)
            DictUnconstraints = {}
            DictUnconstraints['value'] = str(round(dupPerc, 2))
            if farmenew.duplicated().sum()>0:
                DictUnconstraints['details']=nulls_list
            else:
                DictUnconstraints['details']=[]
            Dict['Uniqueness'] = DictUnconstraints
            Uniqueness.append(float(DictUnconstraints['value']))
            value=[100]
            Accvalue=[]
            valAccValue=[]
            isAccuracy= False
            vnulls_list=[]
            AccNulls_list=[]
            framenew = frame.where(pd.notnull(frame), 0)
            #frame = frame.where(pd.notnull(frame), 'None')

            for r in ruleset:
                col=r['column']
                for ru in r['rules']:
                    if ru['rule']=='DataType':
                        if ru['value']=='alphabets':
                            cntalnum=frame[col].str.isalnum().sum()
                            cnttotal=frame[col].count()
                            percen=((cntalnum)/(cnttotal))*100
                            value.append( percen)
                            if(cnttotal!=cntalnum):
                                DNull = {}
                                DNull['column'] =col
                                DNull['ruleType']='DataType'
                                DNull['rulevalue']='alphabets'
                                DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                                r3= frame[~frame[col].str.isalnum()]
                                count=count+1
                                resultId= launchNewId +'RS'+str(count)
                                DNull['outlier']=resultId
                                #saveResultsets(r3.to_dict(),resultId)
                                resDfa2 = r3.where(pd.notnull(r3), 'None')
                                resDfa2.index = resDfa2.index + 2
                                data_dict = resDfa2.to_dict('index')

                                resultsetdict2={}
                                resultsetdict2['resultset'] = resultId
                                resultsetdict2['results'] = data_dict
                                #saveResultsets(data_dict,resultId)
                                resultsetDictlist.append(resultsetdict2)

                                vnulls_list.append(DNull)
                        elif ru['value']=='Numeric':
                            frame[col]=frame[col].astype(str)
                            frame[col]=frame[col].str.replace(".", "",regex=False)
                            cntalnum=frame[col].str.isdigit().sum()
                            cnttotal=frame[col].count()
                            percen=((cntalnum)/(cnttotal))*100
                            value.append(percen)
                            if(cnttotal!=cntalnum):
                                DNull = {}
                                DNull['column'] =col
                                DNull['ruleType']='DataType'
                                DNull['rulevalue']='Numeric'
                                DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                                r4= frame[~frame[col].str.isdigit()]
                                count=count+1
                                resultId= launchNewId +'RS'+str(count)
                                DNull['outlier']=resultId
                                #saveResultsets(r4.to_dict(),resultId)

                                resDfa3 = r4.where(pd.notnull(r4), 'None')
                                resDfa3.index = resDfa3.index + 2
                                data_dict = resDfa3.to_dict('index')

                                resultsetdict3={}
                                resultsetdict3['resultset'] = resultId
                                resultsetdict3['results'] = data_dict
                                #saveResultsets(data_dict,resultId)
                                resultsetDictlist.append(resultsetdict3)
                                vnulls_list.append(DNull)
                    elif ru['rule']=='Length':
                        if (int(float(ru['value']))==frame[col].astype(str).str.len().min()) and  (int(float(ru['value']))==frame[col].astype(str).str.len().max()):
                            value.append(100)
                        else:
                            value.append(100-((((frame[col].astype(str).str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                            DNull = {}
                            DNull['column'] =col
                            DNull['ruleType']='Length'
                            DNull['rulevalue']=ru['value']
                            DNull['ruleMismatchcount'] = str((frame[col].astype(str).str.len() !=int(float(ru['value']))).sum())
                            r5= frame[(frame[col].astype(str).str.len() !=int(float(ru['value'])))]
                            count=count+1
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r5.to_dict(),resultId)

                            resDfa5 = r5.where(pd.notnull(r5), 'None')
                            resDfa5.index = resDfa5.index + 2
                            data_dict = resDfa5.to_dict('index')

                            resultsetdict5={}
                            resultsetdict5['resultset'] = resultId
                            resultsetdict5['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict5)

                            vnulls_list.append(DNull)
                    elif ru['rule']=='ReferenceCDE':
                        print(ru['value'])
                        colname= ru['value'].split('-', 2)[2]
                        print(colname)
                        referenceIdRule = ru['value'].split('-', 2)[1]
                        print(referenceIdRule)
                        referepath=""
                        for referobj in ReferenceObj:
                            if referobj['referenceId']==referenceIdRule:
                                referepath=referobj['referencePath']

                        refpath=referepath
                        #refpath='s3://dquploads/Ref1-8D3NZAE.csv'
                        print(refpath)
                        file_ext= os.path.splitext(os.path.basename(refpath))[1]
                        print(file_ext)
                        if(file_ext=='.csv'):
                            dfref = pd.read_csv(refpath)
                        elif(file_ext=='.xls'):
                            dfref = pd.read_excel(refpath)
                            csv_data = dfref.to_csv(index=None)
                            dfref = pd.read_csv(csv_data)
                        elif(file_ext=='.xlsx'):
                            dfref = pd.read_excel(refpath)
                            csv_data = dfref.to_csv(index=None)
                            dfref = pd.read_csv(csv_data)
                        else:
                            dfref = pd.read_csv(refpath)
                        refCount=0
                        reftotalCount=0
                        refPerc=0
                        refCount=(frame[col].isin(dfref[colname])).sum()
                        reftotalCount=len(frame[col])
                        refPerc= (refCount / reftotalCount)*100
                        nulls_list=[]
                        if (reftotalCount- refCount)>0:

                            DNull = {}
                            DNull['column'] =col
                            DNull['rule'] =str(ru['rule'])+" " +str(ru['value'])
                            DNull['outLiers'] = str(reftotalCount- refCount)
                            r6= frame[~(frame[col].isin(dfref[colname]))]
                            count=count+1
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r6.to_dict(),resultId)
                            resDfa6 = r6.where(pd.notnull(r6), 'None')
                            resDfa6.index = resDfa6.index + 2
                            data_dict = resDfa6.to_dict('index')

                            resultsetdict6={}
                            resultsetdict6['resultset'] = resultId
                            resultsetdict6['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict6)
                            nulls_list.append(DNull)
                        DictInconstraints = {}
                        DictInconstraints['value'] = str(round(refPerc, 2))
                        DictInconstraints['details']=nulls_list
                        Dict['Integrity'] = DictInconstraints
                        Integrity.append(float(DictInconstraints['value']))
                    elif ru['rule'] =='Formula':
                        formula= ru['value']
                        isAccuracy=True

                        fList=[]
                        oList=[]
                        for valcol in formula:
                            fList.append(valcol['cde'])
                            if(valcol['operator']!='NULL'):
                                oList.append(valcol['operator'])
                        framecopy= framenew.copy(deep=True)

                        if oList[0]=='-':
                            col=col
                            col1=fList[0]
                            col2=fList[1]
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            framenew [col1] = pd.to_numeric(framenew[col1], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            framenew [col2] = pd.to_numeric(framenew[col2], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)

                            num_rows = len(framenew[(framenew[col1].astype('float') - framenew[col2].astype('float') == framenew[col].astype('float') )])
                            oulierIndex=framenew.index[(framenew[col1].astype('float') - framenew[col2].astype('float') != framenew[col].astype('float') )].tolist()

                            r7=framecopy.loc[oulierIndex]
                        elif oList[0]=='+':
                            col=col
                            col1=fList[0]
                            col2=fList[1]
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            framenew [col1] = pd.to_numeric(framenew[col1], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            framenew [col2] = pd.to_numeric(framenew[col2], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)

                            num_rows = len(framenew[(framenew[col1].astype('float') + framenew[col2].astype('float') == framenew[col].astype('float') )])
                            oulierIndex=framenew.index[(framenew[col1].astype('float') + framenew[col2].astype('float') != framenew[col].astype('float') )].tolist()

                            r7=framecopy.loc[oulierIndex]
                        elif oList[0]=='*':
                            col=col
                            col1=fList[0]
                            col2=fList[1]


                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')

                            framenew = framenew.replace(np.nan, 0, regex=True)

                            framenew [col1] = pd.to_numeric(framenew[col1], errors='coerce')

                            framenew = framenew.replace(np.nan, 0, regex=True)

                            framenew [col2] = pd.to_numeric(framenew[col2], errors='coerce')

                            framenew = framenew.replace(np.nan, 0, regex=True)

                            num_rows = len(framenew[(framenew[col1].astype('float') * framenew[col2].astype('float') == framenew[col].astype('float') )])
                            oulierIndex=framenew.index[(framenew[col1].astype('float') * framenew[col2].astype('float') != framenew[col].astype('float') )].tolist()
                            print(num_rows)
                            r7=framecopy.loc[oulierIndex]
                        elif oList[0]=='/':
                            col=col
                            col1=fList[0]
                            col2=fList[1]
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            framenew [col1] = pd.to_numeric(framenew[col1], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            framenew [col2] = pd.to_numeric(framenew[col2], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)

                            num_rows = len(framenew[(framenew[col1].astype('float') / framenew[col2].astype('float') == framenew[col].astype('float') )])
                            oulierIndex=framenew.index[(framenew[col1].astype('float') / framenew[col2].astype('float') != framenew[col].astype('float') )].tolist()

                            r7=framecopy.loc[oulierIndex]
                        refCount=0
                        reftotalCount=0
                        refPerc=0
                        refCount=num_rows
                        #r7.index = r7.index + 2
                        reftotalCount=len(framecopy[col])
                        refPerc= (refCount / reftotalCount)*100
                        Accvalue.append(refPerc)
                        nulls_list=[]
                        if(reftotalCount- refCount>0):
                            DNull = {}
                            DNull['column'] =col
                            DNull['rule'] =str(ru['rule'])+ " " +str(fList[0]) +" "+ str(oList[0]) + " " +str(fList[1])
                            DNull['outLiers'] = str(reftotalCount- refCount)


                            count=count+1
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            r7.index = r7.index + 2
                            data_dict = r7.to_dict('index')

                            resultsetdict7={}
                            resultsetdict7['resultset'] = resultId
                            resultsetdict7['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict7)
                            AccNulls_list.append(DNull)
                        #DictInconstraints = {}
                        #DictInconstraints['value'] = str(round(refPerc, 2))
                        #DictInconstraints['details']=nulls_list
                        #Dict['Accuracy'] = DictInconstraints
                    elif ru['rule'] =='Value':
                        ComValue= ru['value']

                        oper=ru['operator']
                        isAccuracy=True
                        framecopy1= framenew.copy(deep=True)
                        if oper=='equalto' or oper=='Shouldbe':

                            if(ComValue.isnumeric()):
                                framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                elnum_rows = len(framenew[framenew[col].astype('float')==float(ComValue)])
                                eloulierIndex=framenew.index[framenew[col].astype('float')!=float(ComValue)].tolist()
                                V7=framecopy1.loc[eloulierIndex]
                            else:
                                print('inside')
                                elnum_rows = len(framenew[str(framenew[col])==(ComValue)])
                                print(elnum_rows)
                                eloulierIndex=framenew.index[str(framenew[col])!=(ComValue)].tolist()
                                V7=framecopy1.loc[eloulierIndex]
                        elif oper=='greaterthanequalto':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')>=float(ComValue)])
                            eloulierIndex=framenew.index[framenew[col].astype('float')<float(ComValue)].tolist()

                            V7=framecopy1.loc[eloulierIndex]
                        elif oper=='lessthanequalto':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')<=float(ComValue)])
                            print(elnum_rows)
                            eloulierIndex=framenew.index[framenew[col].astype('float')>float(ComValue)].tolist()
                            V7=framecopy1.loc[eloulierIndex]
                        elif oper=='greaterthan':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')>float(ComValue)])
                            eloulierIndex=framenew.index[framenew[col].astype('float')<=float(ComValue)].tolist()

                            V7=framecopy1.loc[eloulierIndex]
                        elif oper=='lessthan':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')<float(ComValue)])
                            eloulierIndex=framenew.index[framenew[col].astype('float')>=float(ComValue)].tolist()

                            V7=framecopy1.loc[eloulierIndex]
                        if len(eloulierIndex)>0:
                            DvNull = {}
                            DvNull['column'] =col
                            DvNull['rule'] =str(ru['rule'])+ " " +str(ru['operator']) + " " +str(ru['value'])
                            DvNull['ruleType']='Value'
                            DvNull['rulevalue']=ru['value']
                            DvNull['ruleMismatchcount'] = str(len(eloulierIndex))
                            count=count+1
                            resultId= launchNewId +'RS'+str(count)

                            DvNull['outlier']=resultId
                            #saveResultsets(r5.to_dict(),resultId)

                            resDfv7 = V7.where(pd.notnull(V7), 'None')
                            resDfv7.index = resDfv7.index + 2
                            data_dict = resDfv7.to_dict('index')

                            resultsetdictv7={}
                            resultsetdictv7['resultset'] = resultId
                            resultsetdictv7['results'] = data_dict

                            resultsetDictlist.append(resultsetdictv7)
                            AccNulls_list.append(DvNull)
                        refCount1=elnum_rows

                        reftotalCount1=len(framecopy1[col])
                        refPerc1= (refCount1 / reftotalCount1)*100
                        valAccValue.append(refPerc1)


            res= sum(value)/len(value)
            Dictvconstraints = {}
            Dictvconstraints['value'] = (round(res, 2))
            Dictvconstraints['details']=vnulls_list
            Dict['Validity'] = Dictvconstraints
            Validity.append(float(Dictvconstraints['value']))
            Aresperc=0
            if isAccuracy== True:
                DictInconstraints = {}
                if len(Accvalue)>0:
                    resAcc= sum(Accvalue)/len(Accvalue)
                    Aresperc=resAcc

                if len(valAccValue)>0:
                    resAcc1= sum(valAccValue)/len(valAccValue)
                    Aresperc=resAcc1
                if (len(Accvalue)>0) and (len(valAccValue)>0):
                    Aresperc=(resAcc+resAcc1)/2

                DictInconstraints['value'] = str(round(Aresperc, 2))
                DictInconstraints['details']=AccNulls_list
                Dict['Accuracy'] = DictInconstraints
                Accuracy.append(float(DictInconstraints['value']))
            rules.append(Dict)
        print('calling the saveresultset to s3')
        saveResultsets(resultsetDictlist,(launchNewId))

        resultobject={}
        resultobject['launchId']=launchNewId
        resultobject['keyName']=keyv
        resultobject['results']=rules
        resultobject['isException']='No'
        DictFinal={}
        if len(completeness)>0:
              cPerc= sum(completeness)/len(completeness)
              DictFinal['Completeness'] = str(round(cPerc, 2))
        if len(Accuracy)>0:
              cPerc= sum(Accuracy)/len(Accuracy)
              DictFinal['Accuracy'] = str(round(cPerc, 2))
        if len(Uniqueness)>0:
              cPerc= sum(Uniqueness)/len(Uniqueness)
              DictFinal['Uniqueness'] = str(round(cPerc, 2))
        if len(Integrity)>0:
              cPerc= sum(Integrity)/len(Integrity)
              DictFinal['Integrity'] = str(round(cPerc, 2))
        if len(Validity)>0:
              cPerc= sum(Validity)/len(Validity)
              DictFinal['Validity'] = str(round(cPerc, 2))
        resultobject['aggResults']=DictFinal
        q.put(resultobject)
    except Exception as e:
        resultobject={}
        resultobject['launchId']=launchNewId
        resultobject['keyName']=keyv
        resultobject['results']=rules
        resultobject['aggResults']={}
        resultobject['isException']='Yes'
        resultobject['exception']=  str(e)
        q.put(resultobject)

@app.route('/api/check', methods=['POST'])
def checkifFileExists():
    for txt_path in Path().glob("R_S1U4*"):
        print(txt_path)
    return 'true'

@app.route('/api/uploadSourcegolden', methods=['POST'])
def uploadSourcegolden():
    uploaded_files = request.files.getlist("file[]")
    content =json.loads(request.form.get('data'))
    sourceId= content['sourceId']
    rulesetId= content['rulesetId']
    isMultiSource= content['isMultiSource']
    multiSourceKey= content['multiSourceKey']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['sourceObj']

    settings= content['settings']
    sourceFilename = sourcedetails['sourceFileName']
    sourceCatColumns = sourcedetails['categorialColumns']
    sourcePath=''
    #keyNametoLaunch= sourceCatColumns[0]
    newuploadId=sourceId+'U'+ str(int(getUploadMaxId(sourceId))+1)
    sourcename = os.path.splitext(sourceFilename)[0]
    print(sourcename)
    sourceFilename=newuploadId + sourcename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB(sourceId))

    EntireSourceObj= LaunchEntityRaw['EntireSourceObj']
    EntireSourceList= LaunchEntityRaw['EntireSourceList']
    uploadsObject= EntireSourceObj['uploads']

    print(uploadReason)
    if(uploadReason==''):
        if(isMultiSource=="Yes"):
            if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                        multisrObj= item['multiSourcePath']
                        if multisrObj['multiSourceKey']==multiSourceKey:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file for this multisource key is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
        else:
                if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)

    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
                print(sourceFilename)
                file_ext = os.path.splitext(filename)[1]
                sourceFilename=sourceFilename+file_ext
                print(file_ext)
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    dli=[]
                    sourcePath=sourceFilename
                    file.save(sourcePath)
                    df= df_from_path(sourcePath)
                    # if(file_ext=='.csv'):
                    #     df = pd.read_csv(sourcePath)
                    # elif(file_ext=='.xls'):
                    #     df = pd.read_excel(sourcePath)
                    #     csv_data = df.to_csv(index=None)
                    #     df = pd.read_csv(StringIO(csv_data))
                    # elif(file_ext=='.xlsx'):
                    #     df = pd.read_excel(sourcePath)
                    #     csv_data = df.to_csv(index=None)
                    #     df = pd.read_csv(StringIO(csv_data))
                    # else:
                    #     df = pd.read_csv(sourcePath)
                    dli= list(df.columns.values)

                    sourceavailableColumns=sourcedetails['availableColumns']


                    if dli != sourceavailableColumns:
                        content['errorMsg']= 'The file headers does not match with the configured source'
                        content['errorflag']= 'True'
                        content['errorCode']= '103'
                        return json.dumps(content)
    resultContent={}

    resultContent['launchId']=newuploadId
    resultContent['uploadId']=newuploadId
    resultContent['rulesetId']=rulesetId
    resultContent['sourceFileName']=sourcePath
    resultContent['sourcePath']=sourcePath
    resultContent['uploadDate']=uploadDate
    resultContent['uploadTime']=uploadTime
    resultContent['isexpecteddate']='Yes'
    resultContent['reference']=[]
    resultContent['launchAnalysis']={}
    resultList=[]

    resultContent['AnalysisResultList']=''
    resultContent['isMultiSource']=isMultiSource
    resultContent['multiSource']=[]
    #multiSourcePaths=[]
    if(isMultiSource=="Yes"):
        mpath={}
        mpath['sourcePath']=  sourcePath
        mpath['multiSourceKey']=multiSourceKey
        #multiSourcePaths.append(mpath)
        resultContent['multiSourcePath']=mpath
    content['uploadId']=newuploadId
    content['sourceFilename']=sourceFilename
    uploadsObject.append(resultContent)
    EntireSourceObj['uploads']=uploadsObject
    EntireSourceObj['sourceId']=sourceId
    EntireSourceList.append(EntireSourceObj)
    data={}
    data['LaunchAnalysis']=EntireSourceList
    jsonString= json.dumps(data, default=str)
    json.dump(data, open("ldb.json","w"), default=str)
    content1={}
    content1['errorMsg']= ''
    content1['errorflag']= 'False'
    content1['responseObj']= jsonString

    tempDict={}
    tempDict['sourceId']= sourceId
    tempDict['uploadId'] =newuploadId
    tempDict['source']= sourcedetails
    uploadSourceDetail= {}
    uploadSourceDetail['uploadId'] =newuploadId
    uploadSourceDetail['isMultiSource'] =isMultiSource
    uploadSourceDetail['uploadDate'] =uploadDate
    uploadSourceDetail['uploadTime'] =uploadTime
    uploadSourceDetail['sourceFileName']=sourceFilename
    tempDict['rules']=[]
    tempDict["UploadsHistory"]= []
    tempDict['recentsourceUpload'] = uploadSourceDetail


    inputcontent1={}
    inputcontent1['sourceId']=sourceId
    inputcontent1['uploadId']=newuploadId
    inputcontent1['rulesetId']=rulesetId
    inputcontent1['keyname']=sourceCatColumns




    def long_running_task(**kwargs):
        inputcontent = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", inputcontent)
        rules=[]
        time.sleep(1)
        sourceId = inputcontent['sourceId']
        rulesetId = inputcontent['rulesetId']
        KeyName = inputcontent['keyname']
        uploadId=inputcontent['uploadId']
        #LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId)
        LaunchAnalysisthread(sourceId,rulesetId,KeyName,uploadId)
        print('Completed the main thread function')
    tempDict['stuats'] ='started'
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': inputcontent1})
    thread.start()

    return json.dumps(tempDict, default=str)


@app.route('/getSourcePreview', methods=['GET'])
def getSourcePreview():
        sourceId = request.args.get('sourceId')
        if  '_PS' in sourceId :
            with open('profiledb.json', 'r') as openfile:
                json_object = json.load(openfile)
            data = json_object

            for obj in data['SourceDetailsList']:
                if obj["sourceId"]==sourceId:
                   AnalysisObj=obj
                   break
            sourcepath= AnalysisObj['templateSourcePath']
        elif  '_CS' in sourceId :
            with open('cleandb.json', 'r') as openfile:
                json_object = json.load(openfile)
            data = json_object

            for obj in data['SourceDetailsList']:
                if obj["sourceId"]==sourceId:
                   AnalysisObj=obj
                   break
            sourcepath= AnalysisObj['templateSourcePath']
        else:
            with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)
            data = json_object

            for obj in data['Analysis']:
                if obj["sourceId"]==sourceId:
                   AnalysisObj=obj['source']
                   break
            sourcepath= AnalysisObj['templateSourcePath']

       
        
        df= df_from_path(sourcepath)
        df=df.head()
        resDf = df.where(pd.notnull(df), 'None')
        data_dict = resDf.to_dict('index')
        content={}
        content['sourcePreview']=data_dict
        jsonString = json.dumps(content, default=str)
        return jsonString


@app.route('/getReferencePreview', methods=['GET'])
def getReferencePreview():
        sourceId = request.args.get('sourceId')
        refId = request.args.get('refId')
        with open('db.json', 'r') as openfile:
            json_object = json.load(openfile)

        data = json_object

        for obj in data['Analysis']:
                if obj["sourceId"]==sourceId:
                   AnalysisObj=obj['source']
                   RefObj=obj['reference']
                   for robj in RefObj:
                        if robj["referenceId"]==refId:
                            AnalysisObj=robj
                            break
        sourcepath= AnalysisObj['referencePath']
        #path= 's3://dquploads/Source.csv'
        # file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
        # if(file_ext=='.csv'):
        #     df = pd.read_csv(sourcepath)
        # elif(file_ext=='.xls'):
        #     df = pd.read_excel(sourcepath)
        #     csv_data = df.to_csv(index=None)
        #     df = pd.read_csv(StringIO(csv_data))
        # elif(file_ext=='.xlsx'):
        #     df = pd.read_excel(sourcepath)
        #     csv_data = df.to_csv(index=None)
        #     df = pd.read_csv(StringIO(csv_data))
        # else:
        #     df = pd.read_csv(sourcepath)
        df= df_from_path(sourcepath)
        df=df.head()
        resDf = df.where(pd.notnull(df), 'None')
        data_dict = resDf.to_dict('index')
        content={}
        content['sourcePreview']=data_dict
        jsonString = json.dumps(content, default=str)
        return jsonString

@app.route('/api/converxltocsv', methods=['POST'])
def converxltocsv():
    uploaded_files = request.files.getlist("file[]")
    for file in uploaded_files:
                filename = secure_filename(file.filename)
                if filename != '':
                    file_ext = os.path.splitext(filename)[1]
                    if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                        dli=[]
                        if(file_ext=='.csv'):
                             df = pd.read_csv(file)
                        elif(file_ext=='.xls'):
                             df = pd.read_excel(file)
                             csv_data = df.to_csv(index=None)
                             df = pd.read_csv(StringIO(csv_data))
                        elif(file_ext=='.xlsx'):
                             df = pd.read_excel(file)
                        else:
                            df = pd.read_csv(file)
                        
                        return 'True'

@app.route('/api/getPreviewold', methods=['POST'])
def getPreviewold():
    uploaded_files = request.files.getlist("file[]")
    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                dli=[]
                if(file_ext=='.csv'):
                    df = pd.read_csv(file)
                elif(file_ext=='.xls'):
                    df = pd.read_excel(file, engine="xlrd")
                    csv_data = df.to_csv(index=None)
                    df = pd.read_csv(StringIO(csv_data))
                elif(file_ext=='.xlsx'):
                    df = pd.read_excel(file,index_col=None)
                    csv_data = df.to_csv(index=None)
                    df = pd.read_csv(StringIO(csv_data))
                else:
                    df = pd.read_csv(file)

    print('please check')
    df.index = df.index + 2
    #df=df.head()
    resDf = df.where(pd.notnull(df), 'None')
    data_dict = resDf.to_dict('index')
    content={}
    content['sourcePreview']=data_dict
    jsonString = json.dumps(content, default=str)
    return jsonString

@app.route('/corelationrelationships', methods=['GET'])
def corelationrelationships():
    n=100
    sourcepath = "S1Titanic.xls"
    n=30
    file_ext=".xls"
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)

        csv_data = df.to_csv(index=None)
        df = pd.read_csv(StringIO(csv_data))
    elif(file_ext=='.xlsx'):
        df = pd.read_excel(sourcepath)

        csv_data = df.to_csv(index=None)
        df = pd.read_csv(StringIO(csv_data))
    else:
        df = pd.read_csv(sourcepath)
    if(int(len(df)>800)):
        df=df.head(int(len(df)*(n/100)))
    df_clean = df.select_dtypes(exclude=['object'])
    
    df_clean = df_clean.loc[:,df_clean.apply(pd.Series.nunique) != 1]

    corr_mat = df_clean.corr(method='spearman')
    upper_corr_mat = corr_mat.where(np.triu(np.ones(corr_mat.shape), k=1).astype(bool))
    print(upper_corr_mat)
    response_vars_list = list(upper_corr_mat.columns)

    unique_corr_pairs = upper_corr_mat.unstack().dropna()
    sorted_mat   = unique_corr_pairs.sort_values(ascending=False)
    df_corr_sort = pd.DataFrame(data = sorted_mat, index = sorted_mat.index, columns = ['Corr_Coeff'])
    resp_var=  "Parch"
    df_respvar1 = df_corr_sort.loc[resp_var].sort_values(by = ['Corr_Coeff'], ascending=False)
    dependent_features = list(df_respvar1.index)

    nr_combinations = 2 # Specify the length of combinations to be generated from the list
    dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
    df_relationships = pd.DataFrame(columns = ['response_var','dependent_var1','dependent_var2','relationship'])
    df_relationshipsnew = pd.DataFrame(columns = ['response_var','dependent_var1','dependent_var2','relationship'])
    #for resp_var in response_vars_list:
    print(dep_vars_pairwise_combinations)
    formulaList=[]
    if 1==1:
        rel_dict = {'response_var':[],'dependent_var1':[],'dependent_var2':[],'relationship':[],'entries':[],'percentOfEntries':[]}
        # resp_var_list, dep_var1_list, dep_var2_list,rel_status_list = [], [], [], []
        for pair in dep_vars_pairwise_combinations:
            df_temp = df_clean[[resp_var, pair[0], pair[1]]]
            df_len_threshold = int(len(df_temp)/1.65) # Threshold for valid entries set at 60% of the dataframse size
            print('\nResponse_var:',resp_var,';\tDependent_var1:',pair[0],';\tDependent_var2:',pair[1])
            # Check if the dependant variables have a subtractive relationship for every entry in the dataframe
            try:
               rel_true_array = np.array([True if (df_temp.loc[i,resp_var] == df_temp.loc[i,pair[0]] - df_temp.loc[i,pair[1]]).any() else False for i in range(len(df_temp))])

            except Exception as e:
                print(str(e))
                print('exception')
                rel_true_array=0
            print('sum')
            print(rel_true_array.sum() )
            print('df_len_threshold ')
            print(df_len_threshold )
            print('SUBTRACTION Relationship Identified in ~', 100*np.round(rel_true_array.sum()/len(df_temp), 2) ,'% of the entries')

            if (rel_true_array.sum() > df_len_threshold):
                print('Nr. of TRUE entries:',rel_true_array.sum(),'; \tDataFrame Size:',len(df_temp))
                rel_dict = {'response_var':[],'dependent_var1':[],'dependent_var2':[],'relationship':[],'entries':[],'percentOfEntries':[]}
                rel_dict['response_var'].append(resp_var)
                rel_dict['dependent_var1'].append(pair[0])
                rel_dict['dependent_var2'].append(pair[1])
                rel_dict['relationship'].append('SUBTRACTION')
                rel_dict['entries'].append(rel_true_array.sum())
                rel_dict['percentOfEntries'].append(100*np.round(rel_true_array.sum()/len(df_temp), 2))
                #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                formulaDict={}
                formulaDict['cde']=pair[0]
                formulaDict['operator']='NULL'
                formulaList.append(formulaDict)

                formulaDict1={}
                formulaDict1['cde']=pair[1]
                formulaDict1['operator']='-'
                formulaList.append(formulaDict1)
                print('SUBTRACTION Relationship Identified in ~', 100*np.round(rel_true_array.sum()/len(df_temp), 2) ,'% of the entries')

                df_relationships = df_relationships.append(pd.DataFrame(data = rel_dict), ignore_index=True)
    print(df_relationships)
    f= df_relationships.to_dict('records')
    d={}
    d['relationship'] =f
    d['value']=formulaList
    return d

def GetCorrelationMatrix_v1(df,cols_data_type = 'mixed', method="None",type="",connectiondetails={}):

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
    return corr_matrix_final

# 06/06/2022- Thiru #51 & #57 (Ruleset formula detection issues)
def getCorelationrelationships(df,resp_var, nr_combinations, dep_vars_pairwise_combinations):
    d={}
    formulaList=[]
    d['value']=formulaList
    d['corrMatrix']={}
    rev_combination_found = False
    try:
        
        df_clean = df.select_dtypes(exclude=['object'])
        # df_clean = df_clean.loc[:,df_clean.apply(pd.Series.nunique) != 1]

        valueList=[]
        if 1==1:
            rel_dict = {'response_var':[],'dependent_var1':[],'dependent_var2':[],'relationship':[],'entries':[],'percentOfEntries':[]}
            # resp_var_list, dep_var1_list, dep_var2_list,rel_status_list = [], [], [], []
            for pair in dep_vars_pairwise_combinations:
                rev_combination_found = False
                ind_features = [pair[i] for i in range(nr_combinations)]
                columns = [resp_var] + ind_features
                df_temp = df_clean[columns]
                df_len_threshold = int(len(df_temp)/1.65) # Threshold for valid entries set at 60% of the dataframse size
                
                # Check if the dependant variables have a subtractive relationship for every entry in the dataframe
                try: 
                    addrel_true_array = np.array([True if (df_temp.loc[i,resp_var] == sum([df_temp.loc[i,pair[j]] for j in range(nr_combinations)])).any() else False for i in range(len(df_temp))])
                    if nr_combinations == 2:
                        rel_true_array = np.array([True if (df_temp.loc[i,resp_var] == df_temp.loc[i,pair[0]] - df_temp.loc[i,pair[1]]).any() else False for i in range(len(df_temp))])
                        if rel_true_array.sum() == 0:
                            # change the pair if there is no subtraction relationship with current pair (a,b) -> (b,a)
                            rel_true_array = np.array([True if (df_temp.loc[i,resp_var] == df_temp.loc[i,pair[1]] - df_temp.loc[i,pair[0]]).any() else False for i in range(len(df_temp))])
                            rev_combination_found = True
                        mulrel_true_array = np.array([True if (df_temp.loc[i,resp_var] == df_temp.loc[i,pair[0]] * df_temp.loc[i,pair[1]]).any() else False for i in range(len(df_temp))])
                        divrel_true_array = np.array([True if (df_temp.loc[i,resp_var] == round((df_temp.loc[i,pair[0]] / df_temp.loc[i,pair[1]])*100, 2)).any() or (df_temp.loc[i,resp_var] == round((df_temp.loc[i,pair[1]] / df_temp.loc[i,pair[0]])*100, 2)).any() else False for i in range(len(df_temp))])

                except Exception as e:
                    print('Exception at level 0')
                    addrel_true_array=0
                    rel_true_array=0
                    mulrel_true_array=0
                    divrel_true_array=0
                    
                if (addrel_true_array.sum() > df_len_threshold):
                    print('yes addition realtionship available')
                    #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                    valueDict={}
                    formulaDict={}
                    formulaList = []
                    count = 0

                    valueDict['operand'] = resp_var
                    valueDict['operator'] = '=='
                    valueDict['logic'] = ''
                    valueDict['value'] = ''
                    formulaDict['operand1'] = pair[0]
                    formulaDict['operand2'] = pair[1]
                    formulaDict['operator1'] = "+"
                    formulaDict['operator2'] = ""
                    formulaDict['value'] = ""
                    formulaDict['condition'] = ""
                    formulaDict['index'] = ""
                    formulaDict['group'] = []
                    formulaList.append(formulaDict)
                    
                    tmptext = pair[0] + " + " + pair[1]
                    for x in range(2, nr_combinations):
                        tmptext = tmptext + " + " + pair[x]
                        count += 1
                        if count == 1:
                            formulaDict = {}
                            formulaDict['operand1'] = ""
                            formulaDict['operand2'] = ""
                            formulaDict['operator1'] = ""
                            formulaDict['operator2'] = ""
                            formulaDict['value'] = ""
                            formulaDict['condition'] = ""
                            formulaDict['index'] = ""
                            formulaDict['group'] = []
                        if x % 2 == 0:        
                            formulaDict['operand1'] = pair[x]
                        else:
                            formulaDict['operand2'] = pair[x]
                            
                        if count == 1:
                            formulaDict['operator2'] = "+"
                        if count == 2:
                            formulaDict['operator1'] = "+"

                        if count == 2:
                            formulaList.append(formulaDict)
                            count = 0
                    if count == 1:
                        formulaList.append(formulaDict) 

                    valueDict['formula'] = formulaList
                    formulaText = resp_var + "== (" + tmptext + ")"
                    valueDict['formulaText'] = formulaText
                    valueList.append(valueDict)

                if (nr_combinations == 2):
                    if (rel_true_array.sum() > df_len_threshold):
                        print('yes subtraction realtionship available')
                        #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                        valueDict={}
                        formulaDict={}
                        formulaList = []

                        valueDict['operand'] = resp_var
                        valueDict['operator'] = '=='
                        valueDict['logic'] = ''
                        valueDict['value'] = ''
                        
                        if rev_combination_found:
                            formulaDict['operand1'] = pair[1]
                            formulaDict['operand2'] = pair[0]
                            formulaText = resp_var + "== (" + pair[1] + " - " + pair[0] + ")"
                        else:
                            formulaDict['operand1'] = pair[0]
                            formulaDict['operand2'] = pair[1]
                            formulaText = resp_var + "== (" + pair[0] + " - " + pair[1] + ")"
                        formulaDict['operator1'] = "-"
                        formulaDict['operator2'] = ""
                        formulaDict['value'] = ""
                        formulaDict['condition'] = ""
                        formulaDict['index'] = ""
                        formulaDict['group'] = []
                        formulaList.append(formulaDict)
                        
                        valueDict['formula'] = formulaList
                        # formulaText = resp_var + "== (" + pair[0] + " - " + pair[1] + ")"
                        valueDict['formulaText'] = formulaText
                        valueList.append(valueDict)

                if (nr_combinations == 2):
                    if (mulrel_true_array.sum() > df_len_threshold):
                        print('yes multiplication realtionship available')
                        #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                        valueDict={}
                        formulaDict={}
                        formulaList = []

                        valueDict['operand'] = resp_var
                        valueDict['operator'] = '=='
                        valueDict['logic'] = ''
                        valueDict['value'] = ''
                        
                        formulaDict['operand1'] = pair[0]
                        formulaDict['operand2'] = pair[1]
                        formulaDict['operator1'] = "*"
                        formulaDict['operator2'] = ""
                        formulaDict['value'] = ""
                        formulaDict['condition'] = ""
                        formulaDict['index'] = ""
                        formulaDict['group'] = []
                        formulaList.append(formulaDict)
                        
                        valueDict['formula'] = formulaList
                        formulaText = resp_var + "== (" + pair[0] + " * " + pair[1] + ")"
                        valueDict['formulaText'] = formulaText
                        valueList.append(valueDict)
                if (nr_combinations == 2):
                    if (divrel_true_array.sum() > df_len_threshold):
                        print('yes division realtionship available')
                        #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                        valueDict={}
                        formulaDict={}
                        formulaList = []

                        valueDict['operand'] = resp_var
                        valueDict['operator'] = '=='
                        valueDict['logic'] = ''
                        valueDict['value'] = ''
                        
                        formulaDict['operand1'] = pair[0]
                        formulaDict['operand2'] = pair[1]
                        formulaDict['operator1'] = "/"
                        formulaDict['operator2'] = ""
                        formulaDict['value'] = ""
                        formulaDict['condition'] = ""
                        formulaDict['index'] = ""
                        formulaDict['group'] = []
                        formulaList.append(formulaDict)
                        
                        valueDict['formula'] = formulaList
                        formulaText = resp_var + "== (" + pair[0] + " / " + pair[1] + ")"
                        valueDict['formulaText'] = formulaText
                        valueList.append(valueDict)


        d={}
        d['value']=valueList
        d['corrMatrix']={}
        return d
    except Exception as e:
        print('Exception')
        print(str(e))   
        return d

def getCorelationrelationshipsold(df,resp_var):
    d={}
    formulaList=[]
    d['value']=formulaList
    d['corrMatrix']={}
    try:
        print('inside correlaioships')
        n=30
        df=df.head(int(len(df)*(n/100)))
        df_clean = df.select_dtypes(exclude=['object'])

        df_clean = df_clean.loc[:,df_clean.apply(pd.Series.nunique) != 1]

        corr_mat = df_clean.corr(method='spearman', min_periods=1000)
        upper_corr_mat = corr_mat.where(np.triu(np.ones(corr_mat.shape), k=1).astype(bool))
        response_vars_list = list(upper_corr_mat.columns)
        unique_corr_pairs = upper_corr_mat.unstack().dropna()

        sorted_mat   = unique_corr_pairs.sort_values(ascending=False)
        df_corr_sort = pd.DataFrame(data = sorted_mat, index = sorted_mat.index, columns = ['Corr_Coeff'])
        #resp_var=  'DEPARTURE_DELAY'
        df_respvar1 = df_corr_sort.loc[resp_var].sort_values(by = ['Corr_Coeff'], ascending=False)
        dependent_features = list(df_respvar1.index)

        nr_combinations = 2 # Specify the length of combinations to be generated from the list
        dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
        df_relationships = pd.DataFrame(columns = ['response_var','dependent_var1','dependent_var2','relationship'])
        df_relationshipsnew = pd.DataFrame(columns = ['response_var','dependent_var1','dependent_var2','relationship'])
        #for resp_var in response_vars_list:

        formulaList=[]
        if 1==1:
            rel_dict = {'response_var':[],'dependent_var1':[],'dependent_var2':[],'relationship':[],'entries':[],'percentOfEntries':[]}
            # resp_var_list, dep_var1_list, dep_var2_list,rel_status_list = [], [], [], []
            for pair in dep_vars_pairwise_combinations:
                df_temp = df_clean[[resp_var, pair[0], pair[1]]]
                df_len_threshold = int(len(df_temp)/1.6) # Threshold for valid entries set at 60% of the dataframse size
                # Check if the dependant variables have a subtractive relationship for every entry in the dataframe
                rel_true_array = np.array([True if (df_temp.loc[i,resp_var] == df_temp.loc[i,pair[0]] - df_temp.loc[i,pair[1]]).any() else False for i in range(len(df_temp))])
                addrel_true_array = np.array([True if (df_temp.loc[i,resp_var] == df_temp.loc[i,pair[0]] + df_temp.loc[i,pair[1]]).any() else False for i in range(len(df_temp))])
                mulrel_true_array = np.array([True if (df_temp.loc[i,resp_var] == df_temp.loc[i,pair[0]] * df_temp.loc[i,pair[1]]).any() else False for i in range(len(df_temp))])
                divrel_true_array = np.array([True if (df_temp.loc[i,resp_var] == round((df_temp.loc[i,pair[0]] / df_temp.loc[i,pair[1]])*100, 2)).any() or (df_temp.loc[i,resp_var] == round((df_temp.loc[i,pair[1]] / df_temp.loc[i,pair[0]])*100, 2)).any() else False for i in range(len(df_temp))])
                divsumcount=divrel_true_array.sum()
                print('div threshold : '+ divsumcount)
                if (rel_true_array.sum() > df_len_threshold):
                    print('yes subtraction realtionship available')
                    #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                    formulaDict={}
                    formulaDict['cde']=pair[0]
                    formulaDict['operator']='NULL'
                    formulaList.append(formulaDict)

                    formulaDict1={}
                    formulaDict1['cde']=pair[1]
                    formulaDict1['operator']='-'
                    formulaList.append(formulaDict1)
                if (addrel_true_array.sum() > df_len_threshold):
                    print('yes addition realtionship available')
                    #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                    formulaDict={}
                    formulaDict['cde']=pair[0]
                    formulaDict['operator']='NULL'
                    formulaList.append(formulaDict)

                    formulaDict1={}
                    formulaDict1['cde']=pair[1]
                    formulaDict1['operator']='+'
                    formulaList.append(formulaDict1)
                if (mulrel_true_array.sum() > df_len_threshold):
                    print('yes multiplication realtionship available')
                    #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                    formulaDict={}
                    formulaDict['cde']=pair[0]
                    formulaDict['operator']='NULL'
                    formulaList.append(formulaDict)

                    formulaDict1={}
                    formulaDict1['cde']=pair[1]
                    formulaDict1['operator']='*'
                    formulaList.append(formulaDict1)
                if (divrel_true_array.sum() > df_len_threshold):
                    print('yes division realtionship available')
                    #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                    formulaDict={}
                    formulaDict['cde']=pair[0]
                    formulaDict['operator']='NULL'
                    formulaList.append(formulaDict)

                    formulaDict1={}
                    formulaDict1['cde']=pair[1]
                    formulaDict1['operator']='/'
                    formulaList.append(formulaDict1)
        d={}
        d['value']=formulaList
        d['corrMatrix']=corr_mat.to_dict('records')
        return d
    except Exception as e:
        return d

@app.route('/api/getCorrMatrix_old', methods=['POST']) #GET requests will be blocked
def getCorrMatrix_old():
    content = request.get_json()
    sourcepath= content['sourcepath']
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)

        csv_data = df.to_csv(index=None)
        df = pd.read_csv(StringIO(csv_data))
    elif(file_ext=='.xlsx'):
        df = pd.read_excel(sourcepath)

        csv_data = df.to_csv(index=None)
        df = pd.read_csv(StringIO(csv_data))
    else:
        df = pd.read_csv(sourcepath)


    df.dropna(inplace = True, axis = 'columns',  thresh = 1)
    df_clean = df.select_dtypes(exclude=['object'])
    corr_matrix = df_clean.corr(method='pearson')
    corr_matrix1 = corr_matrix.where(pd.notnull(corr_matrix), 0)


    resdata= corr_mat_highcharts(corr_matrix1)
    jsonString = json.dumps(resdata, default=str)
    return jsonString

@app.route('/api/signup', methods=['POST'])
def signup():
    content= request.get_json()
    userName=content['userName']
    password = content['password']
    name = content['name']
    email = content['email']
    status=content['status']
    isDashboardNotification=content['isDashboardNotification']
    isEMailNotification=content['isEMailNotification']
    res={}
    res["userName"]=userName
    res["email"]=email
    res["name"]=name
    res["password"]=password
    res["role"]=[]
    res['status']=status
    res["registeredDate"]=date.today().strftime("%d/%m/%Y")
    res["lastLoggedin"]=date.today().strftime("%d/%m/%Y")
    res["Manager"]=""
    res["department"]= []
    with open('userdb.json', 'r') as openfile:
            json_object = json.load(openfile)
    UsersList={}
    data=json_object
    for obj in data['UserList']:
        if (obj["userName"]==userName and obj["password"]==password):
            UsersList=obj
            break
    rdata={}

    if not UsersList:
        data['UserList'].append(res)
        json.dump(data, open("userdb.json","w"))
        rdata['responseMsg']= 'User added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'User already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/api/createuser', methods=['POST'])
def createuser():
    content= request.get_json()
    userName=content['userName']
    password = content['password']
    name = content['name']
    email = content['email']
    role = content['role']
    status=content['status']
    department=content['department']
    Utype=content['type']
    userCategory=content['userCategory']
    isDashboardNotification=content['isDashboardNotification']
    isEMailNotification=content['isEMailNotification']
    res={}
    res["userName"]=userName
    res["email"]=email
    res["name"]=name
    res["password"]=password
    res["role"]=role
    res['status']=status
    res["registeredDate"]=date.today().strftime("%d/%m/%Y")
    res["lastLoggedin"]=date.today().strftime("%d/%m/%Y")
    res["Manager"]=""
    res["department"]= department
    res["type"] =Utype
    res["userCategory"] =userCategory
    res["isDashboardNotification"]=isDashboardNotification
    res["isEMailNotification"]=isEMailNotification
    with open('userdb.json', 'r') as openfile:
            json_object = json.load(openfile)
    UsersList={}
    data=json_object
    for obj in data['UserList']:
        if (obj["userName"]==userName and obj["password"]==password):
            UsersList=obj
            break
    rdata={}

    if not UsersList:
        data['UserList'].append(res)
        json.dump(data, open("userdb.json","w"))
        rdata['responseMsg']= 'User added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'User already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/api/createuser', methods=['PUT'])
def edituser():
    content= request.get_json()
    userName=content['userName']
    password = content['password']
    name = content['name']
    email = content['email']
    role = content['role']
    status = content['status']
    department=content['department']
    userCategory=content['userCategory']
    Utype=content['type']
    isDashboardNotification=content['isDashboardNotification']
    isEMailNotification=content['isEMailNotification']

    res={}
    res["userName"]=userName
    res["email"]=email
    res["name"]=name
    res["password"]=password
    res["role"]=role
    res["status"]=status
    res["registeredDate"]=date.today().strftime("%d/%m/%Y")
    res["lastLoggedin"]=date.today().strftime("%d/%m/%Y")
    res["Manager"]=""
    res["department"]= department
    res['type']=Utype

    res["userCategory"] =userCategory
    res["isDashboardNotification"]=isDashboardNotification
    res["isEMailNotification"]=isEMailNotification
    with open('userdb.json', 'r') as openfile:
            json_object = json.load(openfile)
    UsersList={}
    data=json_object
    unalterUsersList=[]
    for obj in data['UserList']:
        if (obj["userName"]==userName):
            UsersList=obj
        else:
            unalterUsersList.append(obj)

    rightsList=[]
    resdata={}
    if not UsersList:
        resdata['errorMsg']= 'User Does Not Exist'
        resdata['errorflag']= 'True'
        jsonString = json.dumps(resdata)
        return jsonString
    else:
        with open('rolesdb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

        rdata=rjson_object
        UsersList=res
        for role1 in role:

            for robj in rdata['Roles']:
                if robj['role']== role1:
                    rightsList= (rightsList+ robj['rights'])
                    break

        #UsersList['role']  =UsersList['role'] + rolename
        unalterUsersList.append(UsersList)
        data['UserList']=unalterUsersList
        json.dump(data, open("userdb.json","w"))
        resdata={}
        resdata['userdetail'] = UsersList
        resdata['roleRights'] = rightsList
        resdata['responseMsg']= 'User updated successfully'
        resdata['errorflag']= 'False'
        jsonString = json.dumps(resdata)
        return jsonString



@app.route('/api/signin', methods=['POST'])
def signin():
    content= request.get_json()
    userName=content['userName']
    password = content['password']
    UsersList={}
    data={}
    resdata={}
    notificationList=[]
    with open('userdb.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['UserList']:
        if (obj["userName"]==userName and obj["password"]==password):
            UsersList=obj
            break
    resdata['userdetail'] = UsersList
    rightsList=[]

    if not UsersList:
        resdata['errorMsg']= 'User Does Not Exist'
        resdata['errorflag']= 'True'
    else:
        with open('rolesdb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

        rdata=rjson_object

        for role in UsersList['role']:
            for robj in rdata['Roles']:
                if robj['role']== role:
                    rightsList= (rightsList+ robj['rights'])
                    break
        with open('notifyDb.json', 'r') as openfile:
           njson_object = json.load(openfile)

        ndata=njson_object
        for notification in ndata["UserNotificationList"]:
            if notification["userName"]==userName:
                notificationList=notificationList+(notification["Notification"])

    res_list = [i for n, i in enumerate(rightsList) if i not in rightsList[n + 1:]]
    rightsList= (list(res_list))
    actualrightslist=[]
    for val in rightsList:
        actualrightslist.append(val["Value"])
    resdata['roleRights'] = actualrightslist
    resdata['notificationCount']=len(notificationList)
    resdata['notification']=notificationList
    jsonString = json.dumps(resdata)
    return jsonString


@app.route('/api/getNotificationList', methods=['POST'])
def getnotificationList():
    content= request.get_json()
    userName=content['userName']
    resdata={}
    notificationList=[]
    if userName!='':
        with open('notifyDb.json', 'r') as openfile:
           njson_object = json.load(openfile)

        ndata=njson_object
        for notification in ndata["UserNotificationList"]:
            if notification["userName"]==userName:
                notificationList=notificationList+(notification["Notification"])

    
    resdata['notificationCount']=len(notificationList)
    resdata['notification']=notificationList
    jsonString = json.dumps(resdata)
    return jsonString


@app.route('/api/addRole', methods=['POST'])
def addRole():
    content= request.get_json()
    userName=content['userName']
    roleName=content['role']
    print(roleName)
    UsersList={}
    unalterUsersList=[]
    data={}
    resdata={}
    with open('userdb.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['UserList']:
        if (obj["userName"]==userName):
            UsersList=obj
        else:
            unalterUsersList.append(obj)

    rightsList=[]

    if not UsersList:
        resdata['errorMsg']= 'User Does Not Exist'
        resdata['errorflag']= 'True'
        jsonString = json.dumps(resdata)
        return jsonString
    else:
        with open('rolesdb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

        rdata=rjson_object
        for role in roleName:
            UsersList['role'].append(role)
            for robj in rdata['Roles']:
                if robj['role']== role:
                    rightsList= (rightsList+ robj['rights'])
                    break

        #UsersList['role']  =UsersList['role'] + rolename
        unalterUsersList.append(UsersList)
        data['UserList']=unalterUsersList
        json.dump(data, open("userdb.json","w"))
        resdata['userdetail'] = UsersList
        resdata['roleRights'] = rightsList
        resdata['responseMsg']= 'User Role added successfully'
        resdata['errorflag']= 'False'
        jsonString = json.dumps(resdata)
        return jsonString


@app.route('/api/getUsers', methods=['GET'])
def getUsers():
    UsersList=[]
    data={}
    resdata={}
    with open('userdb.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['UserList']:
        res={}
        res["userName"]=obj["userName"]
        res["email"]=obj["email"]
        res["name"]=obj["name"]
        res["role"]=obj["role"]
        res["Manager"]=obj["Manager"]
        res["department"]= obj["department"]
        res['status']= obj["status"]
        res['password']= obj["password"]
        res['type']=obj["type"]
        res['userCategory']=obj["userCategory"]
        UsersList.append(res)

    resdata['userList'] = UsersList
    jsonString = json.dumps(resdata)
    return jsonString


@app.route('/api/getRoles', methods=['GET'])
def getRoles():
    #content= request.get_json()
    UsersList={}
    rightsList=[]
    data={}
    resdata={}

    with open('rolesdb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

    rdata=rjson_object
    for robj in rdata['Roles']:
        nobj={}
        nobj['roleName']= robj['role']
        nobj['roleText']= robj['roleDisplay']
        nobj['rights']= robj['rights']
        rightsList.append(nobj)

    resdata['roles']= rightsList
    jsonString = json.dumps(resdata)
    return jsonString

@app.route('/api/getAllRights', methods=['GET'])
def getRights():
    #content= request.get_json()
    UsersList={}
    rightsList=[]
    data={}
    resdata={}

    with open('rightsdb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

    rdata=rjson_object
    for robj in rdata['Rights']:
        rightsList.append(robj)

    resdata['rights']= rightsList
    jsonString = json.dumps(resdata)
    return jsonString

@app.route('/api/getDept', methods=['GET'])
def getDept():
    #content= request.get_json()
    UsersList={}
    rightsList=[]
    data={}
    resdata={}

    with open('deptdb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

    rdata=rjson_object
    for robj in rdata['Department']:
        rightsList.append(robj)

    resdata['department']= rightsList
    jsonString = json.dumps(resdata)
    return jsonString


@app.route('/api/createDept', methods=['POST'])
def createDept():
    content= request.get_json()
    departName=content['departName']
    departText = content['departText']
    res={}
    res["Name"]=departName
    res["Display"]=departText
    res['status']='Active'
    with open('deptdb.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    data=json_object
    print(data['Department'])
    for obj in data['Department']:
        if (obj["Name"]==departName ):
            deptList=obj
            break
    rdata={}

    if not deptList:
        data['Department'].append(res)
        json.dump(data, open("deptdb.json","w"))
        rdata['responseMsg']= 'Department added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'Department already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/api/createDept', methods=['PUT'])
def editDept():
    content= request.get_json()
    departName=content['departName']
    departText = content['departText']
    status=content['status']
    res={}
    res["Name"]=departName
    res["Display"]=departText
    res['status']=status
    with open('deptdb.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    totalDeptObj=[]
    data=json_object
    print(data['Department'])
    for obj in data['Department']:
        if (obj["Name"]==departName ):
            deptList=obj
        else:
            totalDeptObj.append(obj)
    rdata={}

    if not deptList:

        rdata['responseMsg']= 'Department not exist'
        rdata['errorflag']= 'True'
    else:
        rdata['errorMsg']= 'Department updated successfully'
        rdata['errorflag']= 'False'
        totalDeptObj.append(res)
        data['Department']=totalDeptObj
        json.dump(data, open("deptdb.json","w"))
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/api/createRole', methods=['POST'])
def createRole():
    content= request.get_json()
    roleName=content['roleName']
    roleText = content['roleText']
    rights=content['rights']
    res={}
    res["role"]=roleName
    res["roleDisplay"]=roleText
    res['rights']=rights
    with open('rolesdb.json', 'r') as openfile:
            json_object = json.load(openfile)
    roleList={}
    data=json_object
    for obj in data['Roles']:
        if (obj["role"]==roleName ):
            roleList=obj
            break
    rdata={}

    if not roleList:
        data['Roles'].append(res)
        json.dump(data, open("rolesdb.json","w"))
        rdata['responseMsg']= 'role added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'role already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString


@app.route('/api/createRole', methods=['PUT'])
def editRole():
    content= request.get_json()
    roleName=content['roleName']
    roleText = content['roleText']
    rights=content['rights']
    res={}
    res["role"]=roleName
    res["roleDisplay"]=roleText

    with open('rolesdb.json', 'r') as openfile:
            json_object = json.load(openfile)
    roleList={}
    totalroleList=[]
    data=json_object
    for obj in data['Roles']:
        if (obj["role"]==roleName ):
            roleList=obj
        else:
            totalroleList.append(obj)
    rdata={}

    if not roleList:
        rdata['errorMsg']= 'role does not Exist'
        rdata['errorflag']= 'True'

    else:

        my_final_list = (roleList['rights'] + rights)
        res_list = [i for n, i in enumerate(my_final_list) if i not in my_final_list[n + 1:]]
        res['rights']= (list(res_list))
        totalroleList.append(res)
        data['Roles']=totalroleList
        json.dump(data, open("rolesdb.json","w"))
        rdata['responseMsg']= 'role added successfully'
        rdata['errorflag']= 'False'
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/isCharLD', methods=['GET'])
def isCharLD():
  '''Function to check and print if a character in a string is a Letter or Digit'''
  string = request.args.get('string')
  char_list = list(string)
  # Regex to check letters and digits in a string
  regex_str = ("^[a-zA-Z]+$")
  regex_digit = ("^[0-9]+$")
  regex_non_alphanumeric = ('\W+')

  # Compile the Regular Expressions
  l = re.compile(regex_str)
  d = re.compile(regex_digit)
  non_an = re.compile(regex_non_alphanumeric)

  pattern_list = []
  for c in char_list:
    # if (re.search(non_an, c)):
    if (re.findall(non_an, c)):
    # Return # for non-alphanumeric characters
      pattern_list.append("S")
    if (re.search(l, c)):
      pattern_list.append("L")
    if (re.search(d, c)):
      pattern_list.append("D")
  return ''.join(pattern_list)


@app.route('/isWord_Num', methods=['GET'])
def isWord_Num():
  '''Function to check and print if a split up of a phrase contains words or numbers'''
  phrase = request.args.get('phrase')
  string = re.split('[- , ; _]',phrase)
  print("Split Up Phrase:",string)
  pattern_list = []
  for w in string:
    if w.isalpha():
      pattern_list.append("W")
    if w.isnumeric():
      pattern_list.append("N")
    # if w.isalnum():
    #   # Return X for non-alphanumeric characters
    #   pattern_list.append("X")
  return '-'.join(pattern_list)

def AnalyzePattern(phrase):
  '''Function to check and print if a split up of a phrase contains words or numbers'''
  print(phrase)
  phrase=str(phrase)
  string = re.split('[- , ; _]',phrase)
  print("Split Up Phrase:",string)
  pattern_list = []
  for w in string:

    if w.isalpha():
      pattern_list.append("W")
    elif w.isalnum():
      pattern_list.append("S")
    if w.isnumeric():
      pattern_list.append("N")

  return '-'.join(pattern_list)

def AnalyzeMask(string):
  '''Function to check and print if a character in a string is a Letter or Digit'''
  string=str(string)
  char_list = list(string)

  # Regex to check letters and digits in a string
  regex_str = ("^[a-zA-Z]+$")
  regex_digit = ("^[0-9]+$")
  regex_non_alphanumeric = ('\W+')

  # Compile the Regular Expressions
  l = re.compile(regex_str)
  d = re.compile(regex_digit)
  non_an = re.compile(regex_non_alphanumeric)

  pattern_list = []
  for c in char_list:
    # if (re.search(non_an, c)):
    if (re.findall(non_an, c)):
    # Return # for non-alphanumeric characters
      pattern_list.append("S")
    if (re.search(l, c)):
      pattern_list.append("L")
    if (re.search(d, c)):
      pattern_list.append("D")
  return ''.join(pattern_list)

@app.route("/api/maskAnalysis_query", methods=["POST"])
def maskAnalysis_query():
  '''Returns dataframe with row indices matching mask query for a column.
  Mask Analysis Query is best used for categorical columns with < 25 unique values'''
  content = request.get_json()
  sourcepath = content["sourcepath"] # source dataframe
  col = content["column_name"] ## column to query in
  mask_query = content["mask_query_value"] ## Mask Value to query in the datafrme
  df = df_from_path(sourcepath)
  unq_vals = df[col].unique()
  mask_analysis = {}
  for val in unq_vals:
    mask_val = AnalyzeMask(str(val))
    mask_analysis.update({val:mask_val})
  print(col,' - Unique Values Mapping:',mask_analysis)
  df[col + '_MaskAnalysis'] = df[col].map(mask_analysis)
  row_idx = df.index[df[col+'_MaskAnalysis'] == mask_query].to_list()
  return df_to_json(df.iloc[row_idx])

@app.route("/api/cat_col_preview", methods=["POST"])
def cat_col_preview():
    """
    Function to remove NaNs, selected rows or columns
    """
    content = request.get_json()
    cols = content["column_name"] ## List of col_names if category == 'col_complete' or 'col_with_value'
    values = content["values"] ## List of row_numbers if category == row  or column value if category == json
    sourcepath= content["sourcepath"]
    df = df_from_path(sourcepath)
    df_result = df[df[cols].isin(values)]
    return df_to_json(df_result)

def getDBProfileId():
    
    with open('ProfileResults.json', 'r') as openfile:
                    json_object = json.load(openfile)

    data = json_object
    result={}
    IDList=[]
    IDList.append('0')
    for obj in data['Results']:            
        IDList.append(obj["ProfileRID"])
    
    dli2 = [int(s) for s in IDList]   
    
    return (str(max(dli2)+1))


# @app.route('/api/profile', methods=['POST']) #GET requests will be blocked
# def profileAnalysis():
#     content = request.get_json()
#     sourcepath= content['sourcepath']

#     outputdata = json.load(open("ProfileResults.json","r"))
#     OAnalysisList=  outputdata['Results']
#     outputResData={}
#     LDBDataList=[d for d in OAnalysisList if (d["sourcepath"])==sourcepath]
#     if len(LDBDataList)>0:
#         for eachitem in LDBDataList:
#             filename=eachitem["results"]+'.json'
#             rsult={}

#             if os.path.isfile(filename):
#                 with open(filename, 'r') as openfile:
#                     json_object = json.load(openfile)
#                     return json.dumps(json_object, default=str)
#             # rsult = json_object
#             # jsonString = json.dumps(eachitem["results"], default=str)
#             # return jsonString
#     df = df_from_path_profile(sourcepath)
#     print(df)
#     selectedColumns=  list(df.columns.values)
#     df_meta = pd.DataFrame(df.dtypes).reset_index()
#     df_meta.columns = ["column_name", "dtype"]
#     df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
#     df_meta_inferred.columns = ["column_name", "inferred_dtype"]
#     df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
#     conFeat = df_meta[df_meta.dtype == "int64"].column_name.tolist() + df_meta[df_meta.dtype == "float64"].column_name.tolist()
#     print("conFeat:",conFeat)
#     dateFeat = df_meta[df_meta.dtype == "datetime64[ns]"].column_name.tolist() + df_meta[df_meta.dtype == "datetime"].column_name.tolist()
#     print("DateTime Features:", dateFeat)
#     catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
#     print("catFeat:",catFeat)
#     ListResult=[]
#     if(len(conFeat)>0):
#         NumericCorrMatrix = GetCorrelationMatrix(df, 'numeric','pearson')
#     if(len(catFeat)>0):
#         CateoricalCorrMatrix = GetCorrelationMatrix(df, 'categorical' ,'theils_u')
#     catalogueresults=getcatalogueforcolumns(selectedColumns)
    
#     qualityresult= checkDataQuality(df,selectedColumns)
#     c=savequlaitychecktodb(qualityresult,sourcepath)
#     for k in selectedColumns:
#         catalogueresult =  [x for x in catalogueresults if x['Column'] ==k]
        
#         if k in conFeat:
#             df_describe_continuous={}
#             if (df[k].nunique()!=0):
#                 df_describe_continuous = forNumericType([k], df)
#                 # print("\ndf_describe_continous:",df_describe_continuous)
#                 ResMatrix=sorted_corr_matrix_per_col(NumericCorrMatrix,k)
#                 df_describe_continuous['correlationSummary']=ResMatrix
#                 dcatres=[]
#                 for eachresult in catalogueresult:
                   
#                    eachresult['datalineage']=dataLineageforcolumn(k)
#                    dcatres.append(eachresult)
#                 df_describe_continuous['datacatalogue']=dcatres
#                 df_describe_continuous['dq']=[x for x in qualityresult if x['ColumnName'] ==k][0]   
#                 dqvalidityResult= [x for x in qualityresult if x['ColumnName'] ==k][0]
#                 outliervalidity=0 
#                 for y in dqvalidityResult['detail']['Validity']['info']:
#                     outliervalidity=outliervalidity+int(y['OutlierCount'])
#                 att=df_describe_continuous['attributeSummary']
                
#                 att['invalid'] =outliervalidity
#                 df_describe_continuous['attributeSummary']=att    
#             ListResult.append(df_describe_continuous)
#         if k in catFeat:
#             df_describe_categorical={}
#             if(df[k].nunique()!=0):
#                 df_describe_categorical = forStringType([k], df)
#                 if (str(mask_val["unique_values"]).contains("L") for mask_val in df_describe_categorical['maskAnalysis']) and (df_describe_categorical['attributeSummary']['dataType'] == "Numeric"):
#                     print("\n Column for Outlier_Len_Analysis:",k, "DataType:", df_describe_categorical['attributeSummary']['dataType'])
#                     df_describe_categorical['outlier_len_statistics'] = outlier_len_stats_numeric_cols(df_describe_categorical['maskAnalysis'])
#                 ResMatrix=sorted_corr_matrix_per_col(CateoricalCorrMatrix,k)
#                 # print("\n df_describe_categorical:",df_describe_categorical)
#                 df_describe_categorical['correlationSummary'] = ResMatrix
                
#                 dcatres=[]
#                 for eachresult in catalogueresult:
#                    print(k)
#                    print(eachresult)
#                    eachresult['datalineage']=dataLineageforcolumn(k)
#                    dcatres.append(eachresult)
#                 df_describe_categorical['datacatalogue']=dcatres
#                 df_describe_categorical['dq']=[x for x in qualityresult if x['ColumnName'] ==k][0]  
#             ListResult.append(df_describe_categorical)
#         if k in dateFeat:
#             df_describe_datetime = forDateTimeType(k, df)
#             dcatres=[]
#             for eachresult in catalogueresult:
#                    print(k)
#                    print(eachresult)
#                    eachresult['datalineage']=dataLineageforcolumn(k)
#                    dcatres.append(eachresult)
#             df_describe_datetime ['datacatalogue']=dcatres
#             df_describe_datetime ['dq']=[x for x in qualityresult if x['ColumnName'] ==k][0]  
#             ListResult.append(df_describe_datetime)
#     resdata={}
#     resdata['profile'] = ListResult
#     nr_duplicates = len(df[df.duplicated()])
#     resdata['nr_duplicates'] = nr_duplicates
#     resdata['nr_totalrecords'] = len(df)
#     resdata['nr_totalcols']=len(df.columns)
#     inputdata = json.load(open("ProfileResults.json","r"))
#     AnalysisList=  inputdata['Results']
    
#     content['sourcepath']= sourcepath
#     prId=getDBProfileId()
#     content['results']="PR_"+prId
#     content['ProfileRID']=prId
#     AnalysisList.append(content)
#     inputdata['Results'] = AnalysisList
    
#     saveResultsets(resdata,"PR_"+prId)
#     json.dump(inputdata, open("ProfileResults.json","w"), default=str)
#     jsonString = json.dumps(resdata, default=str)
#     return jsonString


@app.route('/api/profileQuality', methods=['POST']) #GET requests will be blocked
def profileQulaityAnalysis():
    content = request.get_json()
    sourcepath= content['sourcepath']
    df = df_from_path_profile(sourcepath)
    selectedColumns=  list(df.columns.values)
    res= checkDataQuality(df,selectedColumns)
    jsonString = json.dumps(res, default=str)
    return jsonString



def columnwiseQualitycheck(frame):
    percent_missing = frame.isnull().sum() * 100 / len(frame)
    missing_value_df = pd.DataFrame({'column_name': frame.columns,
                                 'percent_missing': percent_missing})  
    print(missing_value_df)                                

def columnwiseuniquenesscheck(frame,col):
    dupPerc= 100 -((frame.duplicated([col]).mean())*100)
    return dupPerc

def profileAnalysisRef(sourcepath,df):
   

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
        NumericCorrMatrix = GetCorrelationMatrix_globalRef(df, 'numeric','pearson')
    if(len(catFeat)>0):
        CateoricalCorrMatrix = GetCorrelationMatrix_globalRef(df, 'categorical' ,'theils_u')
    for k in selectedColumns:
        if k in conFeat:
            if (df[k].nunique()!=0):
                df_describe_continuous = forNumericType([k], df)
                # print("\ndf_describe_continous:",df_describe_continuous)
                ResMatrix=sorted_corr_matrix_per_col(NumericCorrMatrix,k)
                df_describe_continuous['correlationSummary']=ResMatrix
                ListResult.append(df_describe_continuous)
        if k in catFeat:
            if(df[k].nunique()!=0):
                df_describe_categorical = forStringType([k], df)
                if (str(mask_val["unique_values"]).contains("L") for mask_val in df_describe_categorical['maskAnalysis']) and (df_describe_categorical['attributeSummary']['dataType'] == "Numeric"):
                    print("\n Column for Outlier_Len_Analysis:",k, "DataType:", df_describe_categorical['attributeSummary']['dataType'])
                    df_describe_categorical['outlier_len_statistics'] = outlier_len_stats_numeric_cols(df_describe_categorical['maskAnalysis'])
                ResMatrix=sorted_corr_matrix_per_col(CateoricalCorrMatrix,k)
                # print("\n df_describe_categorical:",df_describe_categorical)
                df_describe_categorical['correlationSummary'] = ResMatrix
                ListResult.append(df_describe_categorical)
        if k in dateFeat:
            df_describe_datetime = forDateTimeType(k, df)
            ListResult.append(df_describe_datetime)
    resdata={}
    resdata['profile'] = ListResult
    nr_duplicates = len(df[df.duplicated()])
    resdata['nr_duplicates'] = nr_duplicates
    resdata['nr_totalrecords'] = len(df)
    resdata['nr_totalcols']=len(df.columns)
    inputdata = json.load(open("ProfileResults.json","r"))
    AnalysisList=  inputdata['Results']
    content={}
    content['sourcepath']= sourcepath
    prId=getDBProfileId()
    content['results']="PR_"+prId
    content['ProfileRID']=prId
    AnalysisList.append(content)
    inputdata['Results'] = AnalysisList
    print(resdata)
    saveResultsets(resdata,"PR_"+prId)
    json.dump(inputdata, open("ProfileResults.json","w"), default=str)
    jsonString = json.dumps(resdata, default=str)
    return jsonString



def outlier_len_stats_numeric_cols(mask_analysis):
    '''Computes outlier for str'''
    str_mask_val = []
    numeric_mask_val = []

    # str(mask_val["unique_values"]).contains("L") for mask_val in mask_analysis
    for item in mask_analysis:
        if "L" in item["unique_values"]:
            str_mask_val.append(item["unique_values"])
        if "D" in item["unique_values"]:
            numeric_mask_val.append(item["unique_values"])
    print("Str_Mask_values:", str_mask_val)
    print("Numeric Mask Values:", numeric_mask_val)
    ##Compute Length for each type:
    outlier_len_statistics = { }
    if len(numeric_mask_val)>0:
        min_len_en = len(min(numeric_mask_val, key=len))
        max_len_en = len(max(numeric_mask_val, key=len))
    else:
        min_len_en=0
        max_len_en=0
    if len(str_mask_val)>0:
        min_len_es = len(min(str_mask_val, key=len))
        max_len_es = len(max(str_mask_val, key=len))
    else:
        min_len_es = 0
        max_len_es = 0
    outlier_len_statistics["Numeric_vars"] = {"Min" : min_len_en , "Max" : max_len_en}
    outlier_len_statistics["String_vars"] = {"Min" : min_len_es , "Max" : max_len_es}
    return outlier_len_statistics

def forDateTimeType(col, df):
    '''Stat analysis for datetime columns'''
    profileDict = {}
    total_count = len(df)
    null_count = df[col].isna().sum()
    if col in ["Date","date","Datum","datum"]:
        min_val = df[col].min().strftime('%d/%m/%Y')
        max_val = df[col].max().strftime('%d/%m/%Y')
        # print("Max Date:", max_val , "Min Date:", min_val)
        date_series = pd.to_datetime(df[col], errors='coerce')
        df[col] = date_series.dt.strftime('%d/%m/%Y')
        # df[col] = df[col].apply(lambda x: x.strftime(r'%d/%m/%Y'))
        profileDict['maskAnalysis'] = [{"unique_values": "DD/MM/YYYY", "counts": total_count}]
    elif col in ["time", "Time"]:
        min_val = df[col].min().strftime('%H:%M:%S')
        max_val = df[col].max().strftime('%H:%M:%S')
        # print("Max Time:", max_val , "Min Time:", min_val)
        time_series = pd.to_datetime(df[col], errors='coerce')
        df[col] = time_series.dt.strftime('%H:%M:%S')
        # df[col] = df[col].apply(lambda x: x.strftime(r'%H:%M:%S'))
        profileDict['maskAnalysis'] = [{"unique_values": "HH:MM:SS", "counts": total_count}]
    value_counts = df[col].value_counts()
    df_value_counts = pd.DataFrame(value_counts)
    n= len(value_counts)
    df_value_counts = df_value_counts.reset_index()
    df_value_counts.columns = ['unique_values', 'counts']
    freqencyDict= df_value_counts.to_dict('records')
    ## Build output dictionary
    profileDict['column'] = col
    profileDict["LengthStatistics"] = {"Start_Date": min_val, "End_Date": max_val}
    profileDict["attributeSummary"] = {"records":total_count, "dataType":"DateTime", "null_records": null_count,'outliers':null_count,'duplicates':null_count,'invalid':null_count}
    
    profileDict['frequncyAnalysis'] = freqencyDict
    outlierList=[]
    # validList=[]
    outlierPercentDict={}
    profileDict['outliersList'] = outlierList
    # profileDict['validList'] = validList
    profileDict['outliersPercent'] = outlierPercentDict
    att=profileDict['attributeSummary']
    att['outliers'] =  len(outlierList)
    att['duplicates'] = 0
    att['invalid'] = 0
    profileDict['attributeSummary']=att
    return profileDict

def forStringType(column, data):
    columns = data[column]
    value_counts = data[column].value_counts().head(15)
    print('frequency count')
    statisticst=stringStatistics(column,data)
    profileDict={}
    for col in column:
        profileDict['column']=col
        print(col)
    att={}
    for statistics in statisticst:
        att['records']=statistics['Count']
        att['dataType'] =statistics['suggested_dtype']
        att['null_records'] = statistics['NullCount']
        att['outliers'] = 0
        att['duplicates'] = 0
        att['invalid'] =0
        profileDict['attributeSummary']=att
        print (att)
        length={}
        length['Min']=statistics['MinimumLength']
        length['Max'] =statistics['MaximumLength']
        length['Average']=statistics['MeanLength']
        length['Median'] =statistics['MedianLength']
        if statistics['PercentageAlpha'] >0 and statistics['PercentageAlpha']<4:
            statistics['outlierPercentage'] = statistics['PercentageAlpha']
        if statistics['PercentageNum'] >0 and statistics['PercentageNum']<4:
            statistics['outlierPercentage'] = statistics['PercentageNum']
        if statistics['PercentageAlNum'] >0 and statistics['PercentageAlNum']<4:
            statistics['outlierPercentage'] = statistics['PercentageAlNum']
        statistics['duplicateCount']=statistics['Count']-  statistics['UniqueValuesCount']
        profileDict['LengthStatistics']=length
    # converting to df and assigning new names to the columns
    df_value_counts = pd.DataFrame(value_counts)
    n= len(value_counts)
    df_value_counts = df_value_counts.reset_index()
    df_value_counts.columns = ['unique_values', 'counts'] # change column names
    freqencyDict= df_value_counts.to_dict('records')
    profileDict['frequncyAnalysis'] =freqencyDict
    patternList = []
    maskList = []
    maskDict = {}
    patternDict = {}
    for val, count in zip(df_value_counts['unique_values'], df_value_counts['counts']):
        pattern_val = AnalyzePattern(val)
        patternList.append((pattern_val, count))
        mask_val = AnalyzeMask(val)
        maskList.append((mask_val, count))
    newmaskList=[]
    for k1,v1 in maskList: ## for k1,v1,rows in maskList:
        if k1 in maskDict:  ## Append and sum unique key calues in the dictionary
            maskDict[k1] += v1
        else:
            maskDict[k1] = v1
    for k2,v2 in patternList:
      if k2 in patternDict:  ## Append and sum unique key calues in the dictionary
        patternDict[k2] += v2
      else:
        patternDict[k2] = v2
    #print('\nPatternList:', patternList, '\nPatternDict:', patternDict,'Pattern Unique Keys:',patternDict.keys(),'Key_Values:',patternDict.values())
    #print('\n\nMaskList:', maskList, '\nMaskDict:', maskDict, 'Mask Unique Keys:', maskDict.keys(), 'Key_Values:', maskDict.values())
    newmaskList=[]
    for key in maskDict.keys():
        newDict={}
        newDict['unique_values']=key
        newDict['counts']=maskDict[key]
        newmaskList.append(newDict)
    newpatternList=[]
    for key in patternDict.keys():
        newDict={}
        newDict['unique_values']=key
        newDict['counts']=patternDict[key]
        newpatternList.append(newDict)
    profileDict['patternAnalysis'] = newpatternList
    profileDict['maskAnalysis'] = newmaskList
    profileDict['staticalAnalysis'] =statistics
    
    outlierList=[]
    # validList=[]
    outlierPercentDict={}
    profileDict['outliersList'] = outlierList
    # profileDict['validList'] = validList
    profileDict['outliersPercent'] = outlierPercentDict
    att=profileDict['attributeSummary']
    att['outliers'] =  len(outlierList)
    att['duplicates'] = 0
    att['invalid'] = 0
    profileDict['attributeSummary']=att
    if n>1 and n<=5:
        profileDict['isFrequencyChart'] ='True'
    else:
        profileDict['isFrequencyChart'] ='False'

    return profileDict

def forNumericType(column, data):
    columns = data[column]

    profileDict={}
    for col in column:
        profileDict['column']=col

    value_counts = data[column].value_counts().head(15)
    n= len(value_counts)
    statisticst=NumericStatistics(column,data)
    for statistics in statisticst:
        att={}
        att['records']=statistics['Count']
        att['dataType'] =statistics['suggested_dtype']
        att['null_records'] = statistics['NullCount']
        att['outliers'] = statistics['NullCount']
        att['duplicates'] = statistics['NullCount']
        att['invalid'] = statistics['NullCount']
        profileDict['attributeSummary']=att

        length={}
        length['MinimumValue']=statistics['MinimumValue']
        length['MinValLength'] = statistics['minLength']
        length['MaximumValue'] =statistics['MaximumValue']
        length['MaxValLength'] = statistics['MaxLength']
        length['MeanValue']=statistics['MeanValue']
        length['MedianValue'] =statistics['MedianValue']
        length['UniqueValuesCount']=statistics['UniqueValuesCount']
        length['Std_Dev']=statistics['Std_Dev']
        profileDict['valueStatistics']=length
        length1={}

        
        length1['Min']=statistics['minLength']
        length1['Max'] =statistics['MaxLength']
        length1['Average']=statistics['MeanLength']
        length1['Median'] =statistics['MedianLength']
        profileDict['LengthStatistics']=length1

    # converting to df and assigning new names to the columns
    df_value_counts = pd.DataFrame(value_counts)
    df_value_counts = df_value_counts.reset_index()
    df_value_counts.columns = ['unique_values', 'counts'] # change column names
    freqencyDict= df_value_counts.to_dict('records')
    profileDict['frequncyAnalysis'] =freqencyDict
    patternList=[]
    MaskList=[]
    profileDict['patternAnalysis'] =patternList
    profileDict['maskAnalysis'] =MaskList

    profileDict['staticalAnalysis'] =statistics
    outlierList=[]
    # validList=[]
    outlierPercentDict={}
    outlierList, validList, outlierPercentDict = outlierDetection(column, data)
    profileDict['outliersList'] = outlierList
    # profileDict['validList'] = validList
    profileDict['outliersPercent'] = outlierPercentDict
   
    att=profileDict['attributeSummary']
    att['outliers'] =  len(outlierList)
    att['duplicates'] = 0
    att['invalid'] = 0
    profileDict['attributeSummary']=att
    if n>1 and n<=5:
        profileDict['isFrequencyChart'] ='True'
    else:
        profileDict['isFrequencyChart'] ='False'
    return profileDict

def outlierDetection(columns, data):
    outliers = []
    outlierPercentDict = {}
    for column in columns:
        upper_limit = data[column].mean() + data[column].std() * 3
        lower_limit = data[column].mean() - data[column].std() * 3
        outliers = data[(data[column] > upper_limit) | (data[column] < lower_limit)][column].to_list()
        validList = [x for x in data[column].to_list() if x not in outliers]
        
        total_count = data[column].count()
        outlier_cnt = len(outliers)
        outlierPercent = (outlier_cnt/total_count) * 100
        outlierPercentDict["outliers"] = outlierPercent
        normalData_cnt = total_count - outlier_cnt
        normalDataPercent = (normalData_cnt/total_count) * 100
        outlierPercentDict["normal"] = normalDataPercent
    return outliers, validList, outlierPercentDict

def NumericStatistics(conFeat, data):
    conHead = ['Count', 'NullCount','UniqueValuesCount', 'MinimumValue',  'MeanValue', 'MedianValue',  'MaximumValue', 'Std_Dev','minLength','MaxLength','MeanLength','MedianLength']

    conOutDF = pd.DataFrame(index=conFeat, columns=conHead)
    conOutDF.index.name = 'feature_name'
    columns = data[conFeat]
    amt = len(conFeat)
    missNumbers = [''] * amt
    missPercents = [''] * amt

    lenMin = [''] * amt
    outPerc = [''] * amt
    lenMax = [''] * amt
    lenMean = [''] * amt
    lenMdn = [''] * amt
    # COUNT
    count = columns.count()
    conOutDF[conHead[0]] = count

    # NULL Count
    isnull_count = columns.isnull().sum()
    conOutDF[conHead[1]] = isnull_count

    # MISS % - no continuous features have missing data
    percents = [''] * len(conFeat)
    outliers = [''] * len(conFeat)
    for col in columns:
        index = conFeat.index(col)
        anomaly_cut_off = columns[col].std() * 3
        mean = columns[col].mean()
        outliers[conFeat.index(col)] = len(columns[columns[col] < (mean - anomaly_cut_off)]) \
                                       + len(columns[columns[col] > (mean + anomaly_cut_off)])
        

    conOutDF[conHead[2]] = columns.nunique()
    # MINIMUM
    conOutDF[conHead[3]] = columns.min()
    
    # MEAN
    conOutDF[conHead[4]] = round(columns.mean(), 2)
    # MEDIAN
    conOutDF[conHead[5]] = columns.median()
    # MAX
    conOutDF[conHead[6]] = columns.max()
    conOutDF['Data Type']='Numeric'
    conOutDF['suggested_dtype'] = 'Numeric'
    # STANDARD DEVIATION
    conOutDF[conHead[7]] = round(columns.std(), 2)
    columns1 = columns.astype(str)
    columnsSeries = pd.Series(columns1.values.flatten())
    
    conOutDF[conHead[8]] = columnsSeries.str.len().min()
    conOutDF[conHead[9]] = columnsSeries.str.len().max()
    conOutDF[conHead[10]] = round(columnsSeries.str.len().mean(), 2)
    conOutDF[conHead[11]] = columnsSeries.str.len().median()

    
    return  conOutDF.to_dict('records')#.to_json(orient='records')[1:-1].replace('},{', '} {')

def stringStatistics(catFeat, data):
    catHead = ['Count',  'UniqueValuesCount', 'MinimumLength', 'MaximumLength', 'DataType',
               'Alphabetic', 'Numeric', 'Alphanumeric','NullCount','MeanLength','MedianLength',"duplicateCount"]

    catOutDF = pd.DataFrame(index=catFeat, columns=catHead)
    catOutDFMeta = None
    catOutDFPattern = None
    catOutDF.index.name = 'feature_name'

    columns = data[catFeat]
    total_rows = len(data)
    # COUNT
    count = columns.count()
    catOutDF[catHead[0]] = count
    catOutDF[catHead[11]] = count-columns.nunique()
    # NULL COUNT
    null_count = columns.isnull().sum()
    catOutDF[catHead[8]] = null_count

    # CARDINALITY
    catOutDF[catHead[1]] = columns.nunique()

    # preparing arrays for storing data
    amt = len(catFeat)
    missNumbers = [''] * amt
    missPercents = [''] * amt

    lenMin = [''] * amt
    outPerc = [''] * amt
    lenMax = [''] * amt
    lenMean = [''] * amt
    lenMdn = [''] * amt
    dataType = [''] * amt
    dataType2 = [''] * amt
    isAlpha = [''] * amt
    isNum = [''] * amt
    isAlNum = [''] * amt
    pisAlpha = [''] * amt
    pisNum = [''] * amt
    pisAlNum = [''] * amt
    isSpace = [''] * amt
    for col in columns:
        values = columns[col].value_counts()
        col_meta = pd.DataFrame(values).reset_index()
        index = catFeat.index(col)
        missNumbers[index] = columns[col].isnull().sum()
        percent = (missNumbers[index] / total_rows) * 100
        missPercents[index] = round(percent, 2)
        miss = missPercents[index]
        # VALUE LENGTH & TYPE
        col_meta = pd.DataFrame(values).reset_index().rename(columns={'index': 'value', col: 'count'})
        col_meta.insert(0, "feature_name", col)
        col_meta['length'] = col_meta.value.apply(lambda x: len(str(x)))
        col_meta['type'] = col_meta.value.apply(lambda x: type(x))
        
        if index == 0:
            catOutDFMeta = col_meta
        else:
            catOutDFMeta = pd.concat([catOutDFMeta, col_meta], ignore_index=True)
        lenMin[index] = col_meta['length'].min()
        lenMax[index] = col_meta['length'].max()
        lenMean[index]=round(col_meta['length'].mean(),2)
        lenMdn[index]=col_meta['length'].median()
        dataType[index] = col_meta['type'].value_counts().index[0]
        try:
            dataType2[index] = col_meta['type'].value_counts().index[1]
        except Exception as e:
            dataType2[index] = None

        isAlpha[index] = columns[col].str.isalpha().sum()
        isNum[index] = columns[col].str.isnumeric().sum()
        isAlNum[index] = (columns[col].str.isalnum().sum()) - (columns[col].str.isalpha().sum())- (columns[col].str.isnumeric().sum())
        

    catOutDF[catHead[2]] = lenMin
    catOutDF[catHead[3]] = lenMax
    catOutDF[catHead[9]] = lenMean
    catOutDF[catHead[10]] = lenMdn
    catOutDF[catHead[11]] = lenMdn
    catOutDF[catHead[4]] = "string"

    catOutDF[catHead[5]] = isAlpha
    catOutDF[catHead[6]] = isNum
    catOutDF[catHead[7]] = isAlNum
    
    catOutDF['PercentageAlpha'] = round((isAlpha/count)*100,2)
    catOutDF['PercentageNum'] = round((isNum/count)*100,2)
    catOutDF['PercentageAlNum'] = round((isAlNum/count)*100,2)
    catOutDF['suggested_dtype'] = catOutDF.filter(items=['Alphabetic', 'Numeric', 'Alphanumeric'], axis=1) \
        .idxmax(axis=1)
    
    
    return  catOutDF.to_dict('records')#.to_json(orient='records')[1:-1].replace('},{', '} {')

#region multisource
@app.route('/api/getmultisource', methods=['GET'])
def getmultisource():
    #content= request.get_json()
    UsersList={}
    multisourceList=[]
    data={}
    resdata={}

    with open('multisourcedb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

    rdata=rjson_object
    for robj in rdata['multiSourceList']:
        multisourceList.append(robj)

    resdata['multiSourceList']= multisourceList
    jsonString = json.dumps(resdata)
    return jsonString


@app.route('/api/createmultisource', methods=['POST'])
def createmultisource():
    content= request.get_json()
    sourceName=content['label']
    sourceText = content['value']
    res={}
    res["label"]=sourceName
    res["value"]=sourceText
    res['status']='Active'
    with open('multisourcedb.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    data=json_object
    for obj in data['multiSourceList']:
        if (obj["label"]==sourceName ):
            deptList=obj
            break
    rdata={}

    if not deptList:
        data['multiSourceList'].append(res)
        json.dump(data, open("multisourcedb.json","w"))
        rdata['responseMsg']= 'multisource added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'multisource already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/api/createmultisource', methods=['PUT'])
def editmultisource():
    content= request.get_json()
    sourceName=content['label']
    sourceText = content['value']
    status=content['status']
    res={}
    res["label"]=sourceName
    res["value"]=sourceText
    res['status']=status
    with open('multisourcedb.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    totalDeptObj=[]
    data=json_object
    for obj in data['multiSourceList']:
        if (obj["value"]==sourceText ):
            deptList=obj
        else:
            totalDeptObj.append(obj)
    rdata={}

    if not deptList:

        rdata['responseMsg']= 'multiSource not exist'
        rdata['errorflag']= 'True'
    else:
        rdata['errorMsg']= 'multiSource updated successfully'
        rdata['errorflag']= 'False'
        totalDeptObj.append(res)
        data['multiSourceList']=totalDeptObj
        json.dump(data, open("multisourcedb.json","w"))
    jsonString = json.dumps(rdata)
    return jsonString
#endregion multisource

#region userdataCategory

@app.route('/api/createUserCategory', methods=['POST'])
def createUserCategory():
    content= request.get_json()
    Name=content['label']
    Text = content['value']
    department= content['department']
    res={}
    res["category"]=Text
    res["categoryDisplay"]=Name
    res['department']=department
    with open('userCategory.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    data=json_object
    for obj in data['UserCategory']:
        if (obj["category"]==Text ):
            deptList=obj
            break
    rdata={}

    if not deptList:
        data['UserCategory'].append(res)
        json.dump(data, open("userCategory.json","w"))
        rdata['responseMsg']= 'UserCategory added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'UserCategory already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/api/createUserCategory', methods=['PUT'])
def EditUserCategory():
    content= request.get_json()
    Name=content['label']
    Text = content['value']
    department= content['department']
    res={}
    res["category"]=Text
    res["categoryDisplay"]=Name
    res['department']=department
    with open('userCategory.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    data=json_object
    unalteredList=[]
    for obj in data['UserCategory']:
        if (obj["category"]==Text ):
            unalteredList.append(res)
            deptList=obj
        else:
            unalteredList.append(obj)
    rdata={}

    if not deptList:
        rdata['errorMsg']= 'UserCategory does not Exist'
        rdata['errorflag']= 'True'
    else:
        data['UserCategory']=unalteredList
        json.dump(data, open("userCategory.json","w"))
        rdata['responseMsg']= 'UserCategory edited successfully'
        rdata['errorflag']= 'False'
    jsonString = json.dumps(rdata)
    return jsonString

#endregion userdataCategory

#region frequency
@app.route('/api/getfrequency', methods=['GET'])
def getfrequency():
    #content= request.get_json()
    UsersList={}
    multisourceList=[]
    data={}
    resdata={}

    with open('frequencydb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

    rdata=rjson_object
    for robj in rdata['frequencyList']:
        multisourceList.append(robj)

    resdata['frequencyList']= multisourceList
    jsonString = json.dumps(resdata)
    return jsonString


@app.route('/api/createfrequency', methods=['POST'])
def createfrequency():
    content= request.get_json()
    sourceName=content['label']
    sourceText = content['value']
    res={}
    res["label"]=sourceName
    res["value"]=sourceText
    res['status']='Active'
    with open('frequencydb.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    data=json_object
    for obj in data['frequencyList']:
        if (obj["label"]==sourceName ):
            deptList=obj
            break
    rdata={}

    if not deptList:
        data['frequencyList'].append(res)
        json.dump(data, open("frequencydb.json","w"))
        rdata['responseMsg']= 'frequency added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'frequency already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/api/createfrequency', methods=['PUT'])
def editfrequency():
    content= request.get_json()
    sourceName=content['label']
    sourceText = content['value']
    status=content['status']
    res={}
    res["label"]=sourceName
    res["value"]=sourceText
    res['status']=status
    with open('frequencydb.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    totalDeptObj=[]
    data=json_object
    for obj in data['frequencyList']:
        if (obj["value"]==sourceText ):
            deptList=obj
        else:
            totalDeptObj.append(obj)
    rdata={}

    if not deptList:

        rdata['responseMsg']= 'frequency not exist'
        rdata['errorflag']= 'True'
    else:
        rdata['errorMsg']= 'frequency updated successfully'
        rdata['errorflag']= 'False'
        totalDeptObj.append(res)
        data['frequencyList']=totalDeptObj
        json.dump(data, open("frequencydb.json","w"))
    jsonString = json.dumps(rdata)
    return jsonString
#endregion frequency


#region frequency
@app.route('/api/getvendor', methods=['GET'])
def getvendor():
    #content= request.get_json()
    UsersList={}
    vendorList=[]
    data={}
    resdata={}

    with open('datavendordb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

    rdata=rjson_object
    # for robj in rdata['vendorDataTypeList']:
    #     vendorList.append(robj)

    resdata= rdata
    jsonString = json.dumps(resdata)
    return jsonString


@app.route('/api/createvendor', methods=['POST'])
def createvendor():
    content= request.get_json()
    sourceName=content['label']
    sourceText = content['value']
    sourceStatus = content['status']
    sourcevendorlist = content['vendor']
    res={}
    res["label"]=sourceName
    res["value"]=sourceText
    res['status']=sourceStatus
    res['vendor']=sourcevendorlist
    with open('datavendordb.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    data=json_object
    for obj in data['vendorDataTypeList']:
        if (obj["value"]==sourceText):
                deptList=obj
                break
    rdata={}

    if not deptList:
        data['vendorDataTypeList'].append(res)
        json.dump(data, open("datavendordb.json","w"))
        rdata['responseMsg']= 'datavendor added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'datavendor already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/api/createvendor', methods=['PUT'])
def editvendor():
    content= request.get_json()
    sourceName=content['label']
    sourceText = content['value']
    sourceStatus = content['status']
    sourcevendorlist = content['vendor']
    res={}
    res["label"]=sourceName
    res["value"]=sourceText
    res['status']=sourceStatus
    res['vendor']=sourcevendorlist
    with open('datavendordb.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    totalDeptObj=[]
    data=json_object
    for obj in data['vendorDataTypeList']:
        if (obj["value"]==sourceText ):
            deptList=obj
        else:
            totalDeptObj.append(obj)
    rdata={}

    if not deptList:

        rdata['responseMsg']= 'vendor not exist'
        rdata['errorflag']= 'True'
    else:
        rdata['errorMsg']= 'vendor updated successfully'
        rdata['errorflag']= 'False'
        totalDeptObj.append(res)
        data['vendorDataTypeList']=totalDeptObj
        json.dump(data, open("datavendordb.json","w"))
    jsonString = json.dumps(rdata)
    return jsonString
#endregion data vendor



@app.route('/api/completeness', methods=['GET'])
def calculateCompleteness():
    sourcepath= 'Datadump JB test file SHORT.xlsx'
    # file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    # if(file_ext=='.csv'):
    #     df = pd.read_csv(sourcepath)
    # elif(file_ext=='.xls'):
    #     df = pd.read_excel(sourcepath)

    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # elif(file_ext=='.xlsx'):
    #     df = pd.read_excel(sourcepath)

    #     csv_data = df.to_csv(index=None)
    #     df = pd.read_csv(StringIO(csv_data))
    # else:
    #     df = pd.read_csv(sourcepath)
    df= df_from_path(sourcepath)
    KeyName1= 'Entity code'
    dfs= df.groupby([KeyName1])
    count=0
    resultsetDictlist=[]
    try:
        for KeyName, frame in dfs:
            Dict = {}
            Dict[KeyName1] =KeyName
            Dictconstraints = {}
            s1=(len(frame))
            print(s1)
            s2= frame.isnull().sum()
            print(s2)
            v2=(s2/s1)*100
            print(v2)
            v1=100-round(v2,2)
            print(v1)
            Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)
            #rulesList.append(Dict)
            nan_values = frame.isna()
            nan_columns = nan_values.any()
            columns_with_nan = frame.columns[nan_columns].tolist()
            nulls_list=[]
            for k in columns_with_nan:
                DNull = {}
                DNull['column'] =k
                DNull['nullcount'] =str(frame[k].isnull().sum())
                val1= frame[frame[k].isnull()]
                resDf = val1.where(pd.notnull(val1), 'None')

                data_dict = resDf.to_dict('index')
                count=count+1
                launchNewId='test'
                resultId= launchNewId +'RS'+str(count)
                DNull['outlier']=resultId
                resultsetdict={}
                resultsetdict['resultset'] = resultId
                resultsetdict['results'] = data_dict
                #saveResultsets(data_dict,resultId)
                resultsetDictlist.append(resultsetdict)
                nulls_list.append(DNull)
            Dictconstraints['details']=nulls_list


            Dict['completness'] = Dictconstraints
            break
    except Exception as e:
        Dict = {}
        Dict['completness'] = Dictconstraints
    return Dict

@app.route('/api/getUserType', methods=['GET'])
def getUserType():
    UserTypeList=[]
    res={}
    res["label"]='Data Owner'
    res["value"]='DATA_OWNER'
    res['status']='Active'
    UserTypeList.append(res)
    res={}
    res["label"]='Data User'
    res["value"]='DATA_USER'
    res['status']='Active'
    UserTypeList.append(res)
    res={}
    res["label"]='Data Steward'
    res["value"]='DATA_STEWARD'
    res['status']='Active'
    UserTypeList.append(res)
    res={}
    res["label"]='Data Processing Owner'
    res["value"]='DATA_PROCESSING_OWNER'
    res['status']='Active'
    UserTypeList.append(res)
    resdata={}
    resdata['userTypeList']= UserTypeList
    jsonString = json.dumps(resdata)
    return jsonString

#region getAllDataUsers
@app.route('/api/getAllDataUsers', methods=['GET'])
def getAllDataUsers():
    UsersList=[]
    data={}
    resdata={}
    with open('userdb.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['UserList']:
        if( "DATA_USER" in obj["type"]):
            res={}
            res["userName"]=obj["userName"]
            res["email"]=obj["email"]
            res["name"]=obj["name"]
            res["role"]=obj["role"]
            res["Manager"]=obj["Manager"]
            res["department"]= obj["department"]
            res['status']= obj["status"]
            res['password']= obj["password"]
            res['type']=obj["type"]
            UsersList.append(res)

    resdata['userList'] = UsersList
    jsonString = json.dumps(resdata)
    return jsonString

#endregion getAllDataUsers
#region getAllDataOwner
@app.route('/api/getAllDataOwner', methods=['GET'])
def getAllDataOwner():
    UsersList=[]
    data={}
    resdata={}
    with open('userdb.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['UserList']:
        if( "DATA_OWNER" in obj["type"]):
            res={}
            res["userName"]=obj["userName"]
            res["email"]=obj["email"]
            res["name"]=obj["name"]
            res["role"]=obj["role"]
            res["Manager"]=obj["Manager"]
            res["department"]= obj["department"]
            res['status']= obj["status"]
            res['password']= obj["password"]
            res['type']=obj["type"]
            UsersList.append(res)

    resdata['userList'] = UsersList
    jsonString = json.dumps(resdata)
    return jsonString

#endregion getAllDataOwner

#region getAllDataProcessingOwner
@app.route('/api/getAllDataProcOwner', methods=['GET'])
def getAllDataProcOwner():
    UsersList=[]
    data={}
    resdata={}
    with open('userdb.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['UserList']:
        if( "DATA_PROCESSING_OWNER" in obj["type"]):
            res={}
            res["userName"]=obj["userName"]
            res["email"]=obj["email"]
            res["name"]=obj["name"]
            res["role"]=obj["role"]
            res["Manager"]=obj["Manager"]
            res["department"]= obj["department"]
            res['status']= obj["status"]
            res['password']= obj["password"]
            res['type']=obj["type"]
            UsersList.append(res)

    resdata['userList'] = UsersList
    jsonString = json.dumps(resdata)
    return jsonString

#endregion getAllDataProcessingOwner
#region get All user category types
@app.route('/api/getAllUserCategory', methods=['GET'])
def getAllUserCategory():

    with open('userCategory.json', 'r') as openfile:
            json_object = json.load(openfile)
    categoryList=[]
    data=json_object
    for obj in data['UserCategory']:
        res={}
        res["label"]=obj["categoryDisplay"]
        res["value"]=obj["category"]
        categoryList.append(res)
    resdata={}
    resdata['categoryList'] = categoryList
    jsonString = json.dumps(resdata)
    return jsonString

#endregion get All user category types

#region correlation

def GetCorrelationMatrix_globalRef(df,cols_data_type = 'mixed', method="None"):
    
    numeric_cols = list(df.select_dtypes(include='number').columns)
    print('test')
    print(numeric_cols)
    print('cols_data_type')
    print(cols_data_type)
    print('method')
    print(method)
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
        print('test2')
        print(df_clean.head())
        if method == 'pearson':
            corr_matrix = df_clean.corr(method='pearson')
        elif method == 'kendall':
            corr_matrix = df_clean.corr(method='kendall')
        elif method == 'spearman':
            corr_matrix = df_clean.corr(method='spearman')
    if cols_data_type == 'categorical' or len(numeric_cols) == 0:
        df_clean = df[cat_cols]
        print('test3')
        print(df_clean.head())
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
    return corr_matrix_final


#endregion


#region DATA - CLEANING and IMPUTATION


@app.route("/api/df_query_preview", methods=["POST"])
def df_query_preview():
  """Helper function to chain dataframe query conditions and return the queried dataframe"""
  content = request.get_json()
  sourcepath = content["sourcepath"]
  conditions_dict = content["conditions"]
  conditions = conditions_chainer(conditions_dict)
  print("Query Condition Chain:", conditions)
  df = df_from_path(sourcepath)
  df_result = df.query(conditions)
  print("Len of df_result:",len(df_result), "Queried Dataframe:\n",df_result)
  return df_to_json(df_result)


#endregion DATA - CLEANING and IMPUTATION

def getNotificationMaxId(notificationDict):
    if "Notification" in notificationDict:
        existinglsit= notificationDict["Notification"]
    idList=[]
    idList.append(0)
    for notifyList in existinglsit:
        idList.append(notifyList["Id"])
    dli2 = [int(s) for s in idList]
    return (str(max(dli2)+1))



def addAggResults(sourceId,sourcedetails,launchId, uploadId,rulesetId,uploadDate,uploadTime,aggResults,sourcedataName,sourcedataDesc,stype,settings):
    print(sourceId,launchId, uploadId,rulesetId,uploadDate,uploadTime,aggResults)
    resnotify={}
    resnotify["sourceId"]=sourceId
    resnotify["sourceName"]=sourcedataName
    resnotify["sourceDesc"]=sourcedataDesc
    resnotify["sourceType"]=stype
    resnotify["source"]=sourcedetails
    resnotify["settings"]=settings
    resnotifyDict={}
    resnotifyDict["launchId"]=launchId
    resnotifyDict["uploadId"]=uploadId
    resnotifyDict["rulesetId"]= rulesetId
    resnotifyDict["uploadDate"]=uploadDate
    resnotifyDict["uploadTime"]=uploadTime
    resnotifyDict["aggResults"]= aggResults

    resnotify["results"]=[resnotifyDict]
    newList=[]
    with open('AggResultsdb.json', 'r') as openfile:
            json_object = json.load(openfile)
    data=json_object
    aggDict={}
    unalteraggDictList=[]
    for obj in data['AggResults']:
        if (obj["sourceId"]==sourceId):
            aggDict=obj
        else:
            unalteraggDictList.append(obj)


    if not aggDict:
        unalteraggDictList.append(resnotify)
    else:
        aggDict['results'].append(resnotifyDict)

        unalteraggDictList.append(aggDict)

    data['AggResults']=unalteraggDictList
    json.dump(data, open("AggResultsdb.json","w"))

    return 'true'



def addNotification(userName,NotifcationString, status,isRead):
    res={}
    res["userName"]=userName
    resnotify={}
    resnotify["Id"]="1"
    resnotify["status"]=status
    resnotify["text"]=NotifcationString
    resnotify["read"]= isRead
    x = datetime.now()
    resnotify["createdAt"]=str(x)
    newList=[]
    res["Notification"]=[resnotify]
    with open('notifyDb.json', 'r') as openfile:
            json_object = json.load(openfile)
    notificationDict={}
    data=json_object
    unalterNotificationList=[]
    for obj in data['UserNotificationList']:
        if (obj["userName"]==userName):
            notificationDict=obj
        else:
            unalterNotificationList.append(obj)

    rightsList=[]
    resdata={}
    if "Notification" in notificationDict:
        existinglsit= notificationDict["Notification"]
        if not existinglsit:
            newList=[]
            existinglsit= newList.append(resnotify)
        else:
            resnotify["Id"]=getNotificationMaxId(notificationDict)
            existinglsit.append(resnotify)
        res["Notification"]=existinglsit

        unalterNotificationList.append(res)
    else:
        unalterNotificationList.append(res)
    data['UserNotificationList']=unalterNotificationList
    json.dump(data, open("notifyDb.json","w"))

    return 'true'

@app.route("/api/editNotificationStatus", methods=["POST"])
def EditNotificationStatus():
    content = request.get_json()
    id = content["Id"]
    userName = content["userName"]
    isRead = content["isRead"]
    status = content["status"]
    res={}
    res["userName"]=userName

    with open('notifyDb.json', 'r') as openfile:
            json_object = json.load(openfile)
    notificationDictid={}
    notificationDict={}
    data=json_object
    unalterNotificationList=[]

    for obj in data['UserNotificationList']:
        if (obj["userName"]==userName):
            notificationDict=obj
        else:
            unalterNotificationList.append(obj)

    tobeModeifiedList=[]
    if "Notification" in notificationDict:
            for notifyList in notificationDict["Notification"]:
                if (notifyList["Id"]==id):
                    notificationDictid=notifyList
                    notificationDictid["status"]=status
                    notificationDictid["read"]= isRead
                    tobeModeifiedList.append(notificationDictid)
                else:
                    tobeModeifiedList.append(notifyList)
    notificationDict["Notification"]=tobeModeifiedList
    unalterNotificationList.append(notificationDict)

    data['UserNotificationList']=unalterNotificationList
    json.dump(data, open("notifyDb.json","w"))

    return 'true'

def addNotificationList(userNameList,  status,isRead):
    checkList=[]
    for usermsg in userNameList:
        addNotification(usermsg["user"],usermsg["msg"], status,isRead)
    return "true"

#region sourceCategory
@app.route('/api/getsourceCategory', methods=['GET'])
def getsourceCategory():
    #content= request.get_json()
    UsersList={}
    multisourceList=[]
    data={}
    resdata={}

    with open('sourceCategory.json', 'r') as openfile:
            rjson_object = json.load(openfile)

    rdata=rjson_object
    for robj in rdata['sourceCategory']:
        multisourceList.append(robj)

    resdata['sourceCategory']= multisourceList
    jsonString = json.dumps(resdata)
    return jsonString


@app.route('/api/createsourceCategory', methods=['POST'])
def createsourceCategory():
    content= request.get_json()
    sourceName=content['label']
    sourceText = content['value']
    res={}
    res["label"]=sourceName
    res["value"]=sourceText
    res['status']='Active'
    with open('sourceCategory.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    data=json_object
    for obj in data['sourceCategory']:
        if (obj["value"]==sourceText ):
            deptList=obj
            break
    rdata={}

    if not deptList:
        data['sourceCategory'].append(res)
        json.dump(data, open("sourceCategory.json","w"))
        rdata['responseMsg']= 'sourceCategory added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'sourceCategory already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString

@app.route('/api/createsourceCategory', methods=['PUT'])
def editsourceCategory():
    content= request.get_json()
    sourceName=content['label']
    sourceText = content['value']
    status=content['status']
    res={}
    res["label"]=sourceName
    res["value"]=sourceText
    res['status']=status
    with open('sourceCategory.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    totalDeptObj=[]
    data=json_object
    for obj in data['sourceCategory']:
        if (obj["value"]==sourceText ):
            deptList=obj
        else:
            totalDeptObj.append(obj)
    rdata={}

    if not deptList:

        rdata['responseMsg']= 'sourceCategory not exist'
        rdata['errorflag']= 'True'
    else:
        rdata['errorMsg']= 'sourceCategory updated successfully'
        rdata['errorflag']= 'False'
        totalDeptObj.append(res)
        data['sourceCategory']=totalDeptObj
        json.dump(data, open("sourceCategory.json","w"))
    jsonString = json.dumps(rdata)
    return jsonString
#endregion sourceCategory


#cx_Oracle.init_oracle_client(lib_dir=r"D:\downloads\instantclient-basic-windows.x64-19.11.0.0.0dbru\instantclient_19_11")
@app.route("/api/connectoDB", methods=["POST"])
def connectoDB():
    # Connecting to DB
    try:
        connection = cx_Oracle.connect('SYSTEM/lqwd0ercyOTJVkJ5bC3N@orcl')

        print("Successfully connected to Oracle Database")

        stmt="SELECT username FROM all_users ORDER BY username"
        dataframe=pd.read_sql(stmt,con=connection)
        print(dataframe)

        stmt="""SELECT
            table_name, owner
            FROM
            dba_tables
            WHERE
            owner='SYSTEM'
            ORDER BY
            owner, table_name"""
        dataframe=pd.read_sql(stmt,con=connection)
        print(dataframe)

        stmt="select * from retail_datainput"
        dataframe=pd.read_sql(stmt,con=connection)


        resDf = dataframe.where(pd.notnull(dataframe), 'None')
        data_dict = resDf.to_dict('index')

        content={}
        content['schema'] = data_dict
        jsonString = json.dumps(content, default=str)
        return jsonString

    except cx_Oracle.DatabaseError as e:
        print("Problem connecting to Oracle", e)
        rdata={}
        rdata['errorMsg']= 'connection test is not successful'
        rdata['errorflag']= 'True'
        jsonString = json.dumps(rdata)
        return jsonString


@app.route("/api/testconnectivity", methods=["POST"])
def testconnectivity():
    content= request.get_json()
    host=content['host']
    dbName = content['dbName']
    userName = content['userName']
    port = content['port']
    password = content['password']
    rdata={}
    try:
        connStr= userName+"/" + password +"@"+ dbName
        dsn_tns = cx_Oracle.makedsn(host, port, dbName)
        connection = cx_Oracle.connect(user=userName, password=password, dsn=dsn_tns)
        #connection = cx_Oracle.connect(connStr)
        print("Successfully connected to Oracle Database")
        rdata['responseMsg']= 'connection tested successfully'
        rdata['errorflag']= 'False'
        jsonString = json.dumps(rdata)
        return jsonString
    except cx_Oracle.DatabaseError as e:
        print("Problem connecting to Oracle", e)
        print(e)
        rdata['errorMsg']= 'connection test is not successful'
        rdata['errorflag']= 'True'
        print("Successfully connected to Oracle Database")
        rdata['responseMsg']= 'connection tested successfully'
        rdata['errorflag']= 'False'
        jsonString = json.dumps(rdata)
        return jsonString

@app.route("/api/getconnectionSchema", methods=["POST"])
def getconnectionSchema():

    content= request.get_json()
    host=content['host']
    dbName = content['dbName']
    userName = content['userName']
    port = content['port']
    password = content['password']
    try:
        connStr= userName+"/" + password +"@"+ dbName
        dsn_tns = cx_Oracle.makedsn(host, port, dbName)
        connection = cx_Oracle.connect(user=userName, password=password, dsn=dsn_tns)
        print("Successfully connected to Oracle Database")

        stmt="SELECT username FROM all_users ORDER BY username"
        dataframe=pd.read_sql(stmt,con=connection)
        resDf = dataframe.where(pd.notnull(dataframe), 'None')
        data_dict = resDf.to_dict('index')

        content={}
        content['schema'] = data_dict
        jsonString = json.dumps(content, default=str)
        return jsonString
    except cx_Oracle.DatabaseError as e:
        rdata={}
        print("Problem connecting to Oracle", e)
        rdata['errorMsg']= 'connection test is not successful'
        rdata['errorflag']= 'True'
        with open('schema.json', 'r') as openfile:
            json_object = json.load(openfile)
        jsonString = json.dumps(rdata)
        return json_object

@app.route("/api/getconnectionTables", methods=["POST"])
def getconnectionTables():

    content= request.get_json()
    host=content['host']
    dbName = content['dbName']
    userName = content['userName']
    port = content['port']
    password = content['password']
    schema=content['schema']
    try:
        connStr= userName+"/" + password +"@"+ dbName
        dsn_tns = cx_Oracle.makedsn(host, port, dbName)
        connection = cx_Oracle.connect(user=userName, password=password, dsn=dsn_tns)
        print("Successfully connected to Oracle Database")

        stmt="""SELECT
            table_name, owner
            FROM
            dba_tables
            WHERE
            owner=:name
            ORDER BY
            owner, table_name"""
        dataframe=pd.read_sql(stmt, params={'name':schema},con=connection)
        resDf = dataframe.where(pd.notnull(dataframe), 'None')
        data_dict = resDf.to_dict('index')

        content={}
        content['schema'] = schema
        content['tables'] = data_dict
        jsonString = json.dumps(content, default=str)
        return jsonString
    except cx_Oracle.DatabaseError as e:
        rdata={}
        print("Problem connecting to Oracle", e)
        rdata['errorMsg']= 'connection test is not successful'
        rdata['errorflag']= 'True'
        jsonString = json.dumps(rdata)
        with open('tables.json', 'r') as openfile:
            json_object = json.load(openfile)
        jsonString = json.dumps(rdata)
        return json_object

@app.route("/api/getdataframeFromTable", methods=["POST"])
def getdataframeFromTable():

    content= request.get_json()
    host=content['host']
    dbName = content['dbName']
    userName = content['userName']
    port = content['port']
    password = content['password']
    table = content['table']
    try:
        connStr= userName+"/" + password +"@"+ dbName
        dsn_tns = cx_Oracle.makedsn(host, port, dbName)
        connection = cx_Oracle.connect(user=userName, password=password, dsn=dsn_tns)
        print("Successfully connected to Oracle Database")

        stmt="select  * from "+ table
        dataframe=pd.read_sql(stmt,con=connection)
        resDf = dataframe.where(pd.notnull(dataframe), 'None')
        data_dict = resDf.to_dict('index')

        content={}
        content['dataframe'] = data_dict
        jsonString = json.dumps(content, default=str)
        return jsonString
    except cx_Oracle.DatabaseError as e:
        rdata={}
        print("Problem connecting to Oracle", e)
        rdata['errorMsg']= 'connection test is not successful'
        rdata['errorflag']= 'True'
        jsonString = json.dumps(rdata)
        with open('tabledata.json', 'r') as openfile:
            json_object = json.load(openfile)
        return json_object


def getdfFromTable(host,dbName,port,userName,password,table):

    try:
        connStr= userName+"/" + password +"@"+ dbName
        dsn_tns = cx_Oracle.makedsn(host, port, dbName)
        connection = cx_Oracle.connect(user=userName, password=password, dsn=dsn_tns)
        print("Successfully connected to Oracle Database")

        stmt="select  * from "+ table
        dataframe=pd.read_sql(stmt,con=connection)
        return dataframe
    except cx_Oracle.DatabaseError as e:
        print("Problem connecting to Oracle", e)
        df = pd.read_csv("supermarket_sales forumula.csv")
        return df

@app.route('/api/configureSourceForOracle', methods=['POST'])
def configureSourceForOracle():

    content =json.loads(request.form.get('data'))
    sourcedetails= content['source']
    referencedetails= content['reference']
    settings= content['settings']
    data_dict={}
    #source related details
    sourceDataName= sourcedetails['sourceDataName']
    listA = listSourceNames()
    if sourceDataName in listA:
        content['errorMsg']= 'The source name already exists'
        content['errorflag']= 'True'
        content['errorCode']= '101'
        return json.dumps(content, default=str)
    else:
            newSourceId='S'+ str(int(getSourceMaxId())+1)

            if sourcedetails["type"]== "oracle":
                connContent=sourcedetails["connectionDetails"]
                host=connContent['host']
                dbName = connContent['dbName']
                userName = connContent['username']
                port = connContent['port']
                password = connContent['password']
                table = connContent['sourceTableName']
                if dbName != '' and host != '':
                    if userName != '' and password != '':
                        dli=[]
                        try:
                            df = getdfFromTable(host,dbName,port,userName,password,table)
                            if any ('Unnamed' in col for col in df.columns.values.tolist()):
                                    print('Missing Header Columns')
                                    raise pd.errors.ParserError
                        except (pd.errors.ParserError, pd.errors.EmptyDataError):
                            content['errorMsg']= 'Data Parsing Failed. Missing headers AND/OR empty columns'
                            content['errorflag']= 'True'
                            content['errorCode']= '105'
                            return json.dumps(content, default=str)
                        output = classify_columns(df, verbose=2)
                        output = int_cat_datatype_conv(output, df)
                        catFeat=(output['cat_vars'])
                        if not catFeat:
                            if not (output['string_bool_vars']):
                                df_meta = pd.DataFrame(df.dtypes).reset_index()
                                df_meta.columns = ["column_name", "dtype"]
                                df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
                                df_meta_inferred.columns = ["column_name", "inferred_dtype"]
                                df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
                                catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
                            else:
                                catFeat=(output['string_bool_vars'])
                        df=df.head()
                        resDf = df.where(pd.notnull(df), 'None')
                        data_dict = resDf.to_dict('index')
                        dli= list(df.columns.values)
                        sourcedetails['templateSourcePath']= ""
                        sourcedetails['availableColumns'] = dli
                        df_meta = pd.DataFrame(df.dtypes).reset_index()
                        df_meta.columns = ["column_name", "dtype"]
                        df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
                        df_meta_inferred.columns = ["column_name", "inferred_dtype"]
                        df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
                        sourcedetails['categorialColumns'] = catFeat

            content['source']=  sourcedetails
            content['sourcePreview']=data_dict
            data = json.load(open("db.json","r"))
            AnalysisList=  data['Analysis']
            content['sourceId']= newSourceId
            content['rules']=[]
            AnalysisList.append(content)
            data['Analysis'] = AnalysisList
            json.dump(data, open("db.json","w"), default=str)
            content['errorMsg']= ''
            content['errorflag']= 'False'
            return json.dumps(content, default=str)


@app.route('/api/configureSourceForOracle', methods=['PUT'])
def EditconfigureSourceForOracle():
    content =json.loads(request.form.get('data'))
    sourcedetails= content['source']
    sourceId=content['sourceId']
    referencedetails= content['reference']
    settings= content['settings']
    data_dict={}

    datafromdb=  json.loads(removeAEntityDB(sourceId))

    allDBList= datafromdb['Analysis']

    tobeedited= json.loads(GetAEntityDB(sourceId))
    datatobeedited= tobeedited['Analysis']
    rules=datatobeedited['rules']
    #source related details
    sourceDataName= sourcedetails['sourceDataName']
    listA= listSourceNames()
    #sourceDataDescription= sourcedetails['sourceDataDescription']
    sourceFilename = sourcedetails['sourceFileName']
    newSourceId=sourceId
    datasource= datatobeedited['source']
    datareference= datatobeedited['reference']
    sourceFilename= newSourceId+ ""+ sourceFilename
    j=1
    if sourcedetails["type"]== "oracle":
            connContent=sourcedetails["connectionDetails"]
            host=connContent['host']
            dbName = connContent['dbName']
            userName = connContent['username']
            port = connContent['port']
            password = connContent['password']
            table = connContent['sourceTableName']
            if dbName != '' and host != '':
                if userName != '' and password != '':
                    j=j+1
                    dli=[]
                    df = getdfFromTable(host,dbName,port,userName,password,table)
                    output = classify_columns(df, verbose=2)
                    catFeat=(output['cat_vars'])
                    df=df.head()
                    resDf = df.where(pd.notnull(df), 'None')
                    data_dict = resDf.to_dict('index')
                    dli= list(df.columns.values)
                    sourcedetails['templateSourcePath']= ""
                    sourcedetails['availableColumns'] = dli
                    df_meta = pd.DataFrame(df.dtypes).reset_index()
                    df_meta.columns = ["column_name", "dtype"]
                    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
                    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
                    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
                    #catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
                    sourcedetails['categorialColumns'] = catFeat
                    content['sourcePreview']=data_dict

    if(j==1) :
        sourcedetails['templateSourcePath']= ""
        sourcedetails['availableColumns'] = datasource['availableColumns']
        sourcedetails['categorialColumns'] = datasource['categorialColumns']
    content['source']=  sourcedetails

    #reference related details

    content['rules']=rules
    allDBList.append(content)

    data={}
    data['Analysis'] = allDBList
    json.dump(data, open("db.json","w"), default=str)
    return json.dumps(content, default=str)


@app.route('/api/uploadSource1', methods=['POST'])
def uploadSourceoracle1():
    uploaded_files = request.files.getlist("file[]")
    print(uploaded_files)
    content =json.loads(request.form.get('data'))
    sourceId= content['sourceId']
    rulesetId= content['rulesetId']
    isMultiSource= content['isMultiSource']
    multiSourceKey= content['multiSourceKey']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['sourceObj']
    settings= content['settings']
    multiSourceColumn= settings["multiSourceColumn"]
    print(multiSourceColumn)

    # as part of data cleaned files integration
    if 'isCleanedSource' in content:
        isCleanedSource= content['isCleanedSource'] # YES /NO
        cleanedSourcePath= content['cleanedSourcePath']
        if isCleanedSource== "YES":
            uploadReason="cleansedsource"
        print(isCleanedSource)
    else:
        isCleanedSource= 'NO'
        cleanedSourcePath= ''
        print(isCleanedSource)
    
    if 'isOriginalSource' in content:
        isOriginalSource= content['isOriginalSource'] # YES /NO
        OriginalSourcePath= content['OriginalSourcePath']        
    else:
        isOriginalSource= 'NO'
        OriginalSourcePath= ''

    settings= content['settings']
    sourceFilename = sourcedetails['sourceFileName']
    sourceCatColumns = sourcedetails['categorialColumns']
    sourcePath=''
    #keyNametoLaunch= sourceCatColumns[0]
    newuploadId=sourceId+'U'+ str(int(getUploadMaxId(sourceId))+1)
    sourcename = os.path.splitext(sourceFilename)[0]
    print(sourcename)
    sourceFilename=newuploadId + sourcename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB(sourceId))
    print('launch entity in upload source oracle')
    
    EntireSourceObj= LaunchEntityRaw['EntireSourceObj']
    EntireSourceList= LaunchEntityRaw['EntireSourceList']
    uploadsObject= EntireSourceObj['uploads']
 
    typename=""
    connectionDetails={}
    if sourcedetails["type"]== "oracle":
        if sourcedetails["type"]== "oracle":
            typename= "oracle"
            connectionDetails=sourcedetails["connectionDetails"]
            connContent=sourcedetails["connectionDetails"]
            host=connContent['host']
            dbName = connContent['dbName']
            userName = connContent['username']
            port = connContent['port']
            password = connContent['password']
            table = connContent['sourceTableName']
            df = getdfFromTable(host,dbName,port,userName,password,table)
            dli= list(df.columns.values)

            sourceavailableColumns=sourcedetails['availableColumns']
            if dli != sourceavailableColumns:
                content['errorMsg']= 'The file headers does not match with the configured source'
                content['errorflag']= 'True'
                content['errorCode']= '103'
                return json.dumps(content)
    elif isCleanedSource=="YES":
        df= df_from_path(cleanedSourcePath)
        dli= list(df.columns.values)
        sourceavailableColumns=sourcedetails['availableColumns']
        sourcePath=cleanedSourcePath
        if dli != sourceavailableColumns:
                content['errorMsg']= 'The file headers does not match with the configured source'
                content['errorflag']= 'True'
                content['errorCode']= '103'
                return json.dumps(content)
        try:
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
    elif isOriginalSource=="YES":
        df= df_from_path(OriginalSourcePath)
        dli= list(df.columns.values)
        sourceavailableColumns=sourcedetails['availableColumns']
        sourcePath=OriginalSourcePath
        if dli != sourceavailableColumns:
                content['errorMsg']= 'The file headers does not match with the configured source'
                content['errorflag']= 'True'
                content['errorCode']= '103'
                return json.dumps(content)
        try:
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
    else:
        for file in uploaded_files:
            filename = secure_filename(file.filename)
            if filename != '':
                    print(sourceFilename)
                    print(filename)
                    file_ext = os.path.splitext(filename)[1]
                    file_identifier = os.path.splitext(filename)[0]
                    sqllitepath= 'sqlite:///'+file_identifier+'.db'
                    # sqllitepath= 'sqlite:///'+sourceFilename+'.db'
                    # sourceFilename=sourceFilename+file_ext
                    # print(file_ext)
                    if file_ext in app.config['UPLOAD_EXTENSIONS'] :
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

                        
                        df= df_from_path(sourcePath)
                        dli= list(df.columns.values)

                        sourceavailableColumns=sourcedetails['availableColumns']


                        if dli != sourceavailableColumns:
                            content['errorMsg']= 'The file headers does not match with the configured source'
                            content['errorflag']= 'True'
                            content['errorCode']= '103'
                            return json.dumps(content)
                        try:
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

    print(uploadReason)
    if(uploadReason==''):
        if(isMultiSource=="Yes"):
            if(settings['frequency']=="DAILY"):
                
                multiSourceKeyList = []
                multiSourceKeyList.append(multiSourceKey)
                print(df[multiSourceColumn].unique())
                print(multiSourceKeyList)

                for item in uploadsObject:
                        multisrObj= item['multiSourcePath']
                        if df[multiSourceColumn].unique() == multiSourceKeyList:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
                            if multisrObj['multiSourceKey']==multiSourceKey:
                                if payloadDate.date()==actualDate.date():
                                    content['errorMsg']= 'The file for this multisource key is already uploaded for the current date'
                                    content['errorflag']= 'True'
                                    content['errorCode']= '102'
                                    return json.dumps(content)
                        else:
                            content['errorMsg']= 'The file for this multisource key does not match with file unique key'
                            content['errorflag']= 'True'
                            content['errorCode']= '102'
                            return json.dumps(content)
        else:
                if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
    else:
        if uploadReason!="cleansedsource":
            if(isMultiSource!="Yes"):
                print("in")
                if(settings['frequency']=="Daily"):
                    print("in one")
                    editedUploadObj=[]
                    for item in uploadsObject:
                                print("in two")
                                payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                                actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
                                if payloadDate.date()!=actualDate.date():
                                    print("in three")                             
                                    editedUploadObj.append(item)
                    uploadsObject= editedUploadObj
                    print("in four")    
                    print(uploadsObject)         

    # df= df_from_path(mpath)                       


    resultContent={}

    resultContent['launchId']=newuploadId
    resultContent['uploadId']=newuploadId
    resultContent['rulesetId']=rulesetId
    resultContent['sourceFileName']=sourcePath
    resultContent['sourcePath']=sourcePath
    resultContent['type']=typename
    resultContent['connectionDetails']=connectionDetails
    resultContent['uploadDate']=uploadDate
    resultContent['uploadTime']=uploadTime
    resultContent['isexpecteddate']='Yes'
    resultContent['reference']=[]
    resultContent['launchAnalysis']={}
    resultContent['isCleanedSource']=isCleanedSource
    resultContent['isOriginalSource']=isOriginalSource
    resultContent['source']= sourcedetails
    resultContent['settings']=settings
    resultList=[]

    resultContent['AnalysisResultList']=''
    resultContent['isMultiSource']=isMultiSource
    resultContent['multiSource']=[]
    #multiSourcePaths=[]
    if(isMultiSource=="Yes"):
        mpath={}
        mpath['sourcePath']=  sourcePath
        mpath['multiSourceKey']=multiSourceKey
        #multiSourcePaths.append(mpath)
        resultContent['multiSourcePath']=mpath
    content['uploadId']=newuploadId
    content['sourceFilename']=sourceFilename
    uploadsObject.append(resultContent)
    EntireSourceObj['uploads']=uploadsObject
    EntireSourceObj['sourceId']=sourceId
    EntireSourceList.append(EntireSourceObj)
    data={}
    data['LaunchAnalysis']=EntireSourceList
    jsonString= json.dumps(data, default=str)
    json.dump(data, open("ldb.json","w"), default=str)
    content1={}
    content1['errorMsg']= ''
    content1['errorflag']= 'False'
    content1['responseObj']= jsonString

    tempDict={}
    tempDict['sourceId']= sourceId
    tempDict['uploadId'] =newuploadId
    tempDict['source']= sourcedetails
    uploadSourceDetail= {}
    uploadSourceDetail['uploadId'] =newuploadId
    uploadSourceDetail['isMultiSource'] =isMultiSource
    uploadSourceDetail['uploadDate'] =uploadDate
    uploadSourceDetail['uploadTime'] =uploadTime
    uploadSourceDetail['sourceFileName']=sourceFilename
    tempDict['rules']=[]
    tempDict["UploadsHistory"]= []
    tempDict['recentsourceUpload'] = uploadSourceDetail


    inputcontent1={}
    inputcontent1['sourceId']=sourceId
    inputcontent1['uploadId']=newuploadId
    inputcontent1['rulesetId']=rulesetId
    inputcontent1['keyname']=sourceCatColumns


    def long_running_task(**kwargs):
        inputcontent = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", inputcontent)
        rules=[]
        time.sleep(1)
        sourceId = inputcontent['sourceId']
        rulesetId = inputcontent['rulesetId']
        KeyName = inputcontent['keyname']
        uploadId=inputcontent['uploadId']
        #LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId)
        processSourcethread(sourceId,rulesetId,KeyName,uploadId,uploadDate,sourcedetails,settings)
        print('Completed the main thread function')
    tempDict['stuats'] ='started'
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': inputcontent1})
    thread.start()

    return json.dumps(tempDict, default=str)




#region MongoDB_API
## Define MongoDB Cluster URL as Client
client = MongoClient("mongodb+srv://admin:admin@cluster1.zyygq.mongodb.net/admin?retryWrites=false&w=majority")
#endregion

@app.route('/api/mongoDB_ClientHist_log', methods=['GET'])
def mongoDB_ClientHist_log():
    '''Get a list of client_urls entered by the user'''
    data = json.load(open("mongodb_data/mongoDB_ClientHist_log.json","r"))
    return data

@app.route('/api/mongoDB_ClientHist_log_clear', methods=['GET'])
def mongoDB_ClientHist_log_clear():
    '''Clear any existing client_urls entered by the user for the current session'''
    data = {}
    data['ClientHist'] = []
    json.dump(data, open("mongodb_data/mongoDB_ClientHist_log.json","w"), default=str)
    return data

@app.route('/api/mongoDB_Savelog', methods=['GET'])
def mongoDB_Savelog():
    '''Get a list of client_urls entered by the user'''
    data = json.load(open("mongodb_data/mongoDB_SaveLog.json","r"))
    return data

def mongoDB_Clienturl_check(log, client_url):
    '''Verify if a client_url is already entered by the user'''
    if len(log) == 0:
        return "False"
    elif len(log) > 0:
        for element in log:
            if client_url == element["client_url"]:
                return "True"
            else:
                return "False"

@app.route('/api/GetDB_Collections_mongo', methods=['POST'])
def GetDB_Collections():
    '''Get a dictionary of all databases in a cluster, names of collections and field names in each collection'''
    content = request.get_json()
    client_url =  content['client_url']
    if client_url == "":
        client = MongoClient("mongodb+srv://admin:admin@cluster1.zyygq.mongodb.net/admin?retryWrites=false&w=majority")
    else:
        client = MongoClient(client_url)
        try:
            client.admin.command('ping')
            print("Connection Success")
        except ConnectionFailure :
            return {"error": "MongoDB Cluster Server not available"}
    data = json.load(open("mongodb_data/mongoDB_ClientHist_log.json","r"))
    log_old =  data['ClientHist']
    print( "Log Old:", log_old, "Length:", len(log_old))
    msg = mongoDB_Clienturl_check(log_old, client_url)
    print(type(msg), msg)
    if msg == "False" and client_url != "":
        print("New Client URL added: {}".format(client_url))
        log_old.append({"client_url": client_url})
        data['ClientHist'] = log_old
        json.dump(data, open("mongodb_data/mongoDB_ClientHist_log.json","w"), default=str)
    elif msg == "True" :
        print("No new client_urls added to the log")
    dbs_list = [x for x in client.list_database_names() if x not in ['admin', 'local']]
    db_colls_dict = {}
    col_keys = {}
    for db in dbs_list:
        collection_names = client[db].list_collection_names()
        db_colls_dict[db] = collection_names
        for c in collection_names:
            fields = set(list(client[db][c].find_one().keys())) - set(['_id'])
            col_keys[c] = list(fields)
    return {"Cluster_Contents":db_colls_dict, "Databases":list(db_colls_dict.keys()),  "Collection_Contents":col_keys}

def GetDB_Collections_BackEnd(client_url = "mongodb+srv://admin:admin@cluster1.zyygq.mongodb.net/admin?retryWrites=false&w=majority"):
    '''Get a dictionary of all databases in a cluster, names of collections and field names in each collection'''
    client = MongoClient(client_url)
    dbs_list = [x for x in client.list_database_names() if x not in ['admin', 'local']]
    db_colls_dict = {}
    col_keys = {}
    for db in dbs_list:
        collection_names = client[db].list_collection_names()
        db_colls_dict[db] = collection_names
        for c in collection_names:
            fields = set(list(client[db][c].find_one().keys())) - set(['_id'])
            col_keys[c] = list(fields)
    return {"Cluster_Contents":db_colls_dict, "Collection_Contents":col_keys}

def Collection_Exist_query(q_f, db_c, col_fields):
    '''Check if a collection exists in our backend db and Return the database and collection name'''
    for collection in col_fields.keys():
        if q_f in col_fields[collection]:
            res_c = collection
            print("Collection found:", res_c)

    for db in db_c.keys():
        if res_c in db_c[db]:
            res_d = db
            print("Database found:", res_d)

    result = {"db":res_d, "c":res_c}
    print(result)
    return result

@app.route('/api/MongoCluster_query_mongo', methods=['POST'])
def MongoCluster_query():
    '''Launch a query across all Databases and its collections and fields in a MongoDb Cluster'''
    content = request.get_json()
    query =  content['query']
    if content["client_url"] != "":
        dbc_data = GetDB_Collections_BackEnd(content["client_url"])
    else:
        dbc_data = GetDB_Collections_BackEnd()
    db_c = dbc_data["Cluster_Contents"]
    col_keys = dbc_data["Collection_Contents"]
    results = {}
    if type(query) == list and "field" in content.keys(): ## Batch query in selected db and collection
        q_f = content["field"]
        query_result = Collection_Exist_query(q_f, db_c, col_keys)
        db = query_result["db"]
        c = query_result["c"]
        print("Querying selected collection - {} in db - {}".format(db, c))
        res = list(client[db][c].find({q_f:{"$in": query}},{'_id':0}))
        if len(res) != 0:
            results.update({c:res})
    elif "field" not in content.keys(): ## Batch or single query in all dbs and collections
        for db in db_c:
            c_names = db_c[db]
            for c in c_names:
                fields = col_keys[c]
                for f in fields:
                    print('Querying..','db:',db,'\tcollection:',c,'\tfield:', f)
                    if type(query) == str:
                        if "matchcase" in content.keys():
                            res =  list(client[db][c].find({f: query},{'_id':0})) ##Avoid returning Object ID as it creates Error
                        else:
                            res =  list(client[db][c].find({f: re.compile(query+".*", re.IGNORECASE)},{'_id':0}))
                    elif type(query) == list:
                      res = list(client[db][c].find({f:{"$in": query}},{'_id':0}))
                    if len(res) != 0:
                        results.update({c:res})
    print("Results:\n",results)
    if len(results) == 0:
        results = {"msg":"No results from Query"}
    return results

@app.route('/api/MongoDB_Collection_Preview_mongo', methods=['POST'])
def MongoDB_Collection_Preview():
    '''Preview an entrire collection in a database'''
    content = request.get_json()
    client_url = content["client_url"]
    db = content["db"]
    col = content["collection"]
    start_index = content["start_index"]
    end_index = content["end_index"]
    if client_url != "":
        client = MongoClient(client_url)
    else:
        client = MongoClient("mongodb+srv://admin:admin@cluster1.zyygq.mongodb.net/admin?retryWrites=false&w=majority")
    if start_index != "" and end_index != "":
        cursor = client[db][col].find({},{'_id':0})[start_index:end_index]
    else:
        cursor = client[db][col].find({},{'_id':0}, batch_size=1000) #.batch_size(1000) #,
    return {"Preview": list(cursor)}

@app.route('/api/Save_MongoDB_GlobalRef_mango', methods=['GET'])
def Save_MongoDB_GlobalRef():
    '''Save all databases and collections in Backend reference'''
    client = MongoClient("mongodb+srv://admin:admin@cluster1.zyygq.mongodb.net/admin?retryWrites=false&w=majority")
    dbc_data = GetDB_Collections_BackEnd()
    db_c = dbc_data["Cluster_Contents"]
    db_c_saved_paths = []
    for db in db_c:
        c_names = db_c[db]
        for c in c_names:
            cursor = client[db][c].find({},{'_id':0})
            df_mongo = pd.DataFrame(list(cursor))
            df_mongo.dropna(inplace = True, axis = 1, how = "all")
            print(df_mongo.head())
            output_path = "mongodb_data/{}.csv".format(c)
            data = json.load(open("mongodb_data/mongoDB_SaveLog.json","r"))
            log_old =  data['SavedFilesLog']
            new_log_data = {"type":"Global_RefData", "db": db, "collection":c,"outputpath":output_path}
            log_old.append(new_log_data)
            data['SavedFilesLog']= log_old
            json.dump(data, open("mongodb_data/mongoDB_SaveLog.json","w"), default=str)
            print("Saving Cleaned data file at {}".format(output_path))
            df_mongo.to_csv(output_path, index = False)
            db_c_saved_paths.append({"db":db,"collection": c,"outputpath" : output_path})
    return {"Ref_data_files":db_c_saved_paths}

@app.route('/api/MongoDB_Collection_Save_mongo', methods=['POST'])
def MongoDB_Collection_Save():
    '''Preview an entrire collection in a database'''
    content = request.get_json()
    client_url = content["client_url"]
    db = content["db"]
    col = content["collection"]
    output_filename = content["output_filename"]
    source_data_name = os.path.splitext(output_filename)[0]
    if client_url != "":
        client = MongoClient(client_url)
    else:
        client = MongoClient("mongodb+srv://admin:admin@cluster1.zyygq.mongodb.net/admin?retryWrites=false&w=majority")
    cursor = client[db][col].find({},{'_id':0})
    # print(cursor[0:5])
    df_mongo = pd.DataFrame(list(cursor))
    df_mongo.dropna(inplace = True, axis = 1, how = "all")
    print(df_mongo.head())
    output_path = "mongodb_data/{}".format(output_filename)
    if client_url != "":
        data = json.load(open("mongodb_data/mongoDB_SaveLog.json","r"))
        log_old =  data['SavedFilesLog']
        new_log_data = { db : { col : { "type":"User_LocalData", "sourceDataName": source_data_name, "outputpath":output_path } } }
        log_old.append(new_log_data)
        data['SavedFilesLog']= log_old
        json.dump(data, open("mongodb_data/mongoDB_SaveLog.json","w"), default=str)
    print("Saving Cleaned data file at {}".format(output_path))
    df_mongo.to_csv(output_path, index = False)
    return {"Message": "MongoDB Collection Saved", "outputpath" : output_path}

@app.route('/api/MongoDB_Collection_Copy', methods=['POST'])
def MongoDB_Collection_Copy():
    """
    Create New Source for Profile and Data Cleaning Modules
    """
    content = request.get_json()
    sourcepath = content["source_path"]
    client_url = content["client_url"]
    db = content["db"]
    col = content["collection"]
    client = MongoClient(client_url)

    ## Check if db and collection are already existing
    existing_collection_names = client[db].list_collection_names()
    if col in existing_collection_names:
        return {'errorMsg' : "Collection Name already exists. Please input alternative collection name!", "errorCode" : '110', 'errorflag': 'True' }

    try:
        df = df_from_path(sourcepath)
        if any ('Unnamed' in col for col in df.columns.values.tolist()):
            print('Missing Header Columns')
            raise pd.errors.ParserError
    except (pd.errors.ParserError, pd.errors.EmptyDataError):
        content = {}
        content['errorMsg']= 'Data Parsing Failed. Missing headers AND/OR empty columns'
        content['errorflag']= 'True'
        content['errorCode']= '105'
        return json.dumps(content, default=str)
    df_clean = df.where(pd.notnull(df), 'None')
    df_dict = df_clean.to_dict(orient = "records")
    ### Insert new documents into collection
    client[db][col].insert_many(df_dict)
    docs_insert_count = client[db][col].count_documents({})
    return {"message" : "SUCCESS - {} documents uploaded to {} MongoDB Collection".format(docs_insert_count, col)}

@app.route('/api/createNewMongoDBSource_mango', methods=['POST'])
def createNewMongoDBSource():
    """
    Create New Source for Profile and Data Cleaning Modules
    """
    uploaded_files = request.files.getlist("file[]")
    content = json.loads(request.form.get('data'))
    db = content["db"]
    col = content["collection"]
    client_url = content["client_url"]
    client = MongoClient(client_url)

    ## Check if db and collection are already existing
    existing_collection_names = client[db].list_collection_names()
    if col in existing_collection_names:
        return {'errorMsg' : "Collection Name already exists. Please input alternative collection name!", "errorCode" : '110', 'errorflag': 'True' }

    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                file.save(filename)
                try:
                    df = df_from_path(filename)
                    if any ('Unnamed' in col for col in df.columns.values.tolist()):
                            error_msg = "Missing headers AND/OR empty columns"
                            print(error_msg)
                            raise pd.errors.ParserError ## ['.','$','*',':','|','?']
                    if (col.str.contains('.|$|*|:|?|<|>',case=False, regex=True) for col in df.columns.values.tolist()):
                            error_msg = "Column Header names in source dataset contain one or more prohibited characters '. $ * < > : | ? '. Please see https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names"
                            print(error_msg)
                            raise pd.errors.ParserError
                except (pd.errors.ParserError, pd.errors.EmptyDataError):
                    content = {}
                    content['errorMsg']= 'Data Parsing Failed. {}'.format(error_msg)
                    content['errorflag']= 'True'
                    content['errorCode']= '105'
                    return json.dumps(content, default=str)
    df_clean = df.where(pd.notnull(df), 'None')
    os.remove(filename)
    df_dict = df_clean.to_dict(orient = "records")
    ### Insert new documents into collection
    client[db][col].insert_many(df_dict)
    docs_insert_count = client[db][col].count_documents({})
    return {"message" : "SUCCESS - {} documents uploaded to {} MongoDB Collection".format(docs_insert_count, col)}
#end region MongoDB_API




##region Domain_Type_Matching
### Reference Cols dictionary - {"Domain" : [list of col names]}
ref_cols_dict = {
"Gender" : ['sex','gender'],
"iata_code": ["airport", "iata"],
"city": ["cities", "city"]
}

@app.route('/api/DomainType_Identifier', methods=['POST'])
def DomainType_Identifier():
  '''Checks if columns in a source dataframe match with our domain categories (field names) in our MongoDB reference data'''
  content = request.get_json()
  sourcepath = content["sourcepath"]
  df = df_from_path(sourcepath)
  df_source_cols = list(df.select_dtypes(include=['category','object']).columns)
  # df_source_cols += ['PICKY.KICKY', 'D-MM-YYYY', 'FN,LN'] ## test
  print("Str columns:", df_source_cols)
  result = {}

  for col in df_source_cols:
      split_text = re.split('[_ - , ; .]', col.lower())
      print("Column:",col,".\tSplit Col Name:", split_text)
      for key in ref_cols_dict.keys():
          ref_cols = ref_cols_dict[key]
          for c in split_text:
              if c in ref_cols:
                  print("\tColumn '{}' containing '{}' - Found in Domain '{}' containing {}".format(col, c, key, ref_cols))
                  result.update({col:key})

  ### Store unique values of domain type identified columns in a list
  unq_vals_source_cols = {}
  for column in result.keys():
      unq_vals_source_cols[column] = list(df[column].dropna().unique())

  return {"Domain_Matches": result, "Unique_values":unq_vals_source_cols}
#end region Domain_Type_Matching




@app.route('/api/GetSourceResults', methods=['POST']) #GET requests will be blocked
def GetSourceResults():
    content = request.get_json()    
    userName=content['userName']
    userCategory=content['userCategory']
    userDepartment= content['department']    
    data={}
    endResults={}
    Totalcompleteness=[]
    TotalAccuracy=[]
    TotalUniqueness=[]
    TotalIntegrity=[]
    TotalValidity=[]
    completeness=[]
    Accuracy=[]
    Uniqueness=[]
    Integrity=[]
    Validity=[]
    with open('AggResultsdb.json', 'r') as openfile:
        json_object = json.load(openfile)

    data = json_object
    newuploadId=''
    jsonString=''
    result={}
    isavailableflag=False
    processedResult=[]
    for obj in data['AggResults']:
        print(obj)
        if isSourceValid(userName,userCategory,userDepartment,obj) :
            sourcedetailList={}
            sourcedetailList["sourceId"]=obj["sourceId"]
            sourcedetailList["sourceName"]=obj["sourceName"]
            sourcedetailList["sourceDesc"]=obj["sourceDesc"]
            sourcedetailList["sourceType"]=obj["sourceType"]
            sourceResults=[]
            completeness=[]
            Accuracy=[]
            Uniqueness=[]
            Integrity=[]
            Validity=[]
            datelist=[]
            datelist=(([((datetime.strptime(d['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")).date()) for d in obj["results"] ]))                
            test_list = list(set(datelist))
            for objdate in test_list: 
                LDBDataList=[d for d in obj["results"] if (datetime.strptime(d["uploadDate"],"%Y-%m-%dT%H:%M:%S.%fZ")).date()==objdate]
                obj1={}
                for objitem in LDBDataList:
                    obj1=objitem
                print(obj1)
                if obj1:           
                        UploadObjDict={}
                        UploadObjDict['uploadDate']= obj1['uploadDate']
                        UploadObjDict['uploadTime']= obj1['uploadTime']
                        
                        finalResults=obj1['aggResults']
                        if 'Completeness' in finalResults:
                            completeness.append(float(finalResults['Completeness']))
                            Totalcompleteness.append(float(finalResults['Completeness']))
                        else:
                            finalResults['Completeness']="NA"
                        if 'Accuracy' in finalResults:
                            Accuracy.append(float(finalResults['Accuracy']))
                            TotalAccuracy.append(float(finalResults['Accuracy']))
                        else:
                            finalResults['Accuracy']="NA"
                        if 'Uniqueness' in finalResults:
                            Uniqueness.append(float(finalResults['Uniqueness']))
                            TotalUniqueness.append(float(finalResults['Uniqueness']))
                        else:
                            finalResults['Uniqueness']="NA"
                        if 'Integrity' in finalResults:
                            Integrity.append(float(finalResults['Integrity']))
                            TotalIntegrity.append(float(finalResults['Integrity']))
                        else:
                            finalResults['Integrity']="NA"
                        if 'Validity' in finalResults:
                            Validity.append(float(finalResults['Validity']))
                            TotalValidity.append(float(finalResults['Validity']))
                        else:
                            finalResults['Validity']="NA"
                        UploadObjDict['results']=finalResults
                        sourceResults.append(UploadObjDict)
            sourcedetailList["detailedResults"]= sourceResults

            DictFinal={}
            if len(completeness)>0:
                cPerc= sum(completeness)/len(completeness)
                DictFinal['Completeness'] = str(round(cPerc, 2))
            else:
                DictFinal['Completeness'] = "NA"
            if len(Accuracy)>0:
                cPerc= sum(Accuracy)/len(Accuracy)
                DictFinal['Accuracy'] = str(round(cPerc, 2))
            else:
                DictFinal['Accuracy'] = "NA"
            if len(Uniqueness)>0:
                cPerc= sum(Uniqueness)/len(Uniqueness)
                DictFinal['Uniqueness'] = str(round(cPerc, 2))
            else:
                DictFinal['Uniqueness'] = "NA"
            if len(Integrity)>0:
                cPerc= sum(Integrity)/len(Integrity)
                DictFinal['Integrity'] = str(round(cPerc, 2))
            else:
                DictFinal['Integrity'] = "NA"
            if len(Validity)>0:
                cPerc= sum(Validity)/len(Validity)
                DictFinal['Validity'] = str(round(cPerc, 2))
            else:
                DictFinal['Validity'] = "NA"
            sourcedetailList["AggSubResults"]= DictFinal
            processedResult.append(sourcedetailList)
    DictEnd={}
    if len(Totalcompleteness)>0:
                cPerc= sum(Totalcompleteness)/len(Totalcompleteness)
                DictEnd['Completeness'] = str(round(cPerc, 2))
    else:
        DictEnd['Completeness'] = "NA"
    if len(TotalAccuracy)>0:
                cPerc= sum(TotalAccuracy)/len(TotalAccuracy)
                DictEnd['Accuracy'] = str(round(cPerc, 2))
    else:
        DictEnd['Accuracy'] = "NA"
    if len(TotalUniqueness)>0:
                cPerc= sum(TotalUniqueness)/len(TotalUniqueness)
                DictEnd['Uniqueness'] = str(round(cPerc, 2))
    else:
        DictEnd['Uniqueness'] = "NA"
    if len(TotalIntegrity)>0:
                cPerc= sum(TotalIntegrity)/len(TotalIntegrity)
                DictEnd['Integrity'] = str(round(cPerc, 2))
    else:
        DictEnd['Integrity'] = "NA"
    if len(TotalValidity)>0:
                cPerc= sum(TotalValidity)/len(TotalValidity)
                DictEnd['Validity'] = str(round(cPerc, 2))
    else:
        DictEnd['Validity'] = "NA"
    endResults["Aggresults"]=DictEnd
    endResults["detailedResults"]=processedResult
    jsonString = json.dumps(endResults, default=str)
    return jsonString



@app.route("/api/checkformulaEditor", methods=["GET"])
def checkformulaeditor():
    ru={}
    ru['rule'] ='Formula'
    ru['type']='CONDITIONAL1'
    LISTV=[]
    count=0
    ru['value']=LISTV
    listdict={"cde": "Pclass",
              "operator": "NULL"}
    LISTV.append(listdict)
    listdict1={
              "cde": "SibSp",
              "operator": "-"
            }
    LISTV.append(listdict1)
    framenew= pd.read_csv("testfile.csv")
    if ru['rule'] =='Formula':
                        rulename=""
                        isAccuracy=True
                        if ru['type']=='CONDITIONAL':
                            #framenew= framenew.dropna()

                            formula= ru['value']
                            querystring =GetQueryString(formula)
                            invertedQString=GetinvertQueryString(formula)
                            framecopy= framenew.copy(deep=True)
                            print(querystring)
                            print(invertedQString)
                            print(col)
                            print('conditional vala main loop')

                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                framenew=getFormatteddf(formula,framenew)
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                print('actual complie with formule: ')
                                print( num_rows)
                                idf2= framenew.query(invertedQString)
                                x = pd.concat([df2, idf2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                print(r7.head())
                                num_rows= len(framecopy[col])-len(r7)
                                print('actual count with formule: ')
                                print( num_rows)
                                #print(len(df3))
                                #print((df3.index).tolist())
                            rulename=querystring
                        else:
                            formula= ru['value']
                            isAccuracy=True
                            fList=[]
                            oList=[]
                            for valcol in formula:
                                fList.append(valcol['cde'])
                                if(valcol['operator']!='NULL'):
                                    oList.append(valcol['operator'])
                            framecopy= framenew.copy(deep=True)
                            iter = len(oList)
                            print(fList)
                            print(oList)
                            col='Parch'
                            if oList[0]=='-':
                                for i in range(iter):
                                    # col = col
                                    print('inside {i}' , i)
                                    for j in range(len(fList)):
                                        framenew[fList[j]] = pd.to_numeric(framenew[fList[j]], errors='coerce')
                                        framenew = framenew.replace(np.nan, 0, regex=True)
                                        add = framenew[fList[j]].astype('float')
                                    print (add.head())
                                    add -= add
                                    print (add.head())
                                col = 'Parch'
                                framenew[col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                num_rows = len(framenew[(add == framenew[col].astype('float'))])
                                oulierIndex = framenew.index[(add != framenew[col].astype('float'))].tolist()

                                r7 = framecopy.loc[oulierIndex]
                                print (r7.head())
                            elif oList[0] == '+':
                                for i in range(iter):
                                    # col = col
                                    # globals()[f"col{i + 1}"] = fList[i]
                                    framenew[fList[i]] = pd.to_numeric(framenew[fList[i]], errors='coerce')
                                    framenew = framenew.replace(np.nan, 0, regex=True)
                                    add = framenew[fList[i]].astype('float')
                                    add += add
                                col = col
                                framenew[col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                num_rows = len(framenew[(add == framenew[col].astype('float'))])
                                oulierIndex = framenew.index[(add != framenew[col].astype('float'))].tolist()

                                r7 = framecopy.loc[oulierIndex]
                            elif oList[0] == '*':
                                for i in range(iter):
                                    # col = col
                                    # globals()[f"col{i + 1}"] = fList[i]
                                    framenew[fList[i]] = pd.to_numeric(framenew[fList[i]], errors='coerce')
                                    framenew = framenew.replace(np.nan, 0, regex=True)
                                    add = framenew[fList[i]].astype('float')
                                    add *= add
                                col = col
                                framenew[col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                num_rows = len(framenew[(add == framenew[col].astype('float'))])
                                oulierIndex = framenew.index[(add != framenew[col].astype('float'))].tolist()

                                r7 = framecopy.loc[oulierIndex]
                            elif oList[0] == '/':
                                for i in range(iter):
                                    # col = col
                                    # globals()[f"col{i + 1}"] = fList[i]
                                    framenew[fList[i]] = pd.to_numeric(framenew[fList[i]], errors='coerce')
                                    framenew = framenew.replace(np.nan, 0, regex=True)
                                    add = framenew[fList[i]].astype('float')
                                    add /= add
                                col = col
                                framenew[col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                num_rows = len(framenew[(add == framenew[col].astype('float'))])
                                oulierIndex = framenew.index[(add != framenew[col].astype('float'))].tolist()

                                r7 = framecopy.loc[oulierIndex]
                            test = ''.join(map(str, itertools.chain.from_iterable(zip(fList, oList))))
                            rulename = str(ru['rule'] + " " + test + str(fList[-1]))
                        refCount=0
                        reftotalCount=0
                        refPerc=0
                        refCount=num_rows
                        #r7.index = r7.index + 2
                        reftotalCount=len(framecopy[col])
                        if reftotalCount!=0:
                            refPerc= (refCount / reftotalCount)*100
                            #Accvalue.append(refPerc)
                        nulls_list=[]
                        if(reftotalCount- refCount>0):
                            DNull = {}
                            DNull['column'] =col
                            DNull['rule'] = rulename
                            DNull['outLiers'] = str(reftotalCount- refCount)


                            count=count+1
                            #resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                            #DNull['outlier']=resultId
                            r7 = r7.where(pd.notnull(r7), 'None')
                            r7.index = r7.index + 2
                            data_dict = r7.to_dict('index')

                            resultsetdict7={}
                            resultsetdict7['resultset'] = 2
                            resultsetdict7['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            #resultsetDictlist.append(resultsetdict7)
                            #AccNulls_list.append(DNull)
    return resultsetdict7


@app.route("/api/checkformulaEditor1", methods=["GET"])
def checkformulaeditor1():
    ru={}
    ru['rule'] ='Formula'
    ru['type']='CONDITIONAL'
    LISTV=[]
    count=0
    ru['value']=LISTV
    listdict={"cde": "Pclass",
              "operator": "NULL"}
    LISTV.append(listdict)
    listdict1={
              "cde": "SibSp",
              "operator": "-"
            }
    LISTV.append(listdict1)


    col='Sex'
    
    rcond=[]
    # rcondl={
    #               "start": "(",
    #               "cde1": "Pclass",
    #               "operator1": "==",
    #               "cde2": "",
    #               "value": "1",
    #               "end": ")",
    #               "condition": "",
    #               "operator2": ""
    #             }
    rcondl={
        "start": "(",
        "cde1": "Pclass",
        "operator1": "+",
        "cde2": "Pclass",
        "value": "",
        "end": "",
        "condition": "",
        "operator2": "=="
      }
    rcondl1={
        "start": "",
        "cde1": "",
        "operator1": "",
        "cde2": "",
        "value": "2",
        "end": ")",
        "condition": "",
        "operator2": ""
      }
    rcond.append(rcondl)
    rcond.append(rcondl1)
    rl={
              "logic": "If",
              "conditions":rcond,
              "retcde1": "Sex",
              "retoperator": "==",
              "retcde2": "",
              "retvalue": "male"
            }
    LISTV1=[]
    LISTV1.append(rl)        
    ru['value']= LISTV1   
    framenew= pd.read_csv("testfile.csv")
    if ru['rule'] =='Formula':
                        rulename=""
                        isAccuracy=True
                        if ru['type']=='CONDITIONAL':
                            #framenew= framenew.dropna()

                            formula= ru['value']
                            querystring =GetQueryStringnew(formula)
                            invertedQString=GetinvertQueryString(formula)
                            framecopy= framenew.copy(deep=True)
                            print(querystring)
                            print(invertedQString)
                            print(col)
                            print('conditional vala main loop')

                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                framenew=getFormatteddf(formula,framenew)
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                print('actual complie with formule: ')
                                print( num_rows)
                                idf2= framenew.query(invertedQString)
                                x = pd.concat([df2, idf2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                print(r7.head())
                                num_rows= len(framecopy[col])-len(r7)
                                print('actual count with formule: ')
                                print( num_rows)
                                #print(len(df3))
                                #print((df3.index).tolist())
                            rulename=querystring
                        else:
                            formula= ru['value']
                            isAccuracy=True
                            fList=[]
                            oList=[]
                            for valcol in formula:
                                fList.append(valcol['cde'])
                                if(valcol['operator']!='NULL'):
                                    oList.append(valcol['operator'])
                            framecopy= framenew.copy(deep=True)
                            iter = len(oList)
                            print(fList)
                            print(oList)
                            col='Parch'
                            if oList[0]=='-':
                                for i in range(iter):
                                    # col = col
                                    print('inside {i}' , i)
                                    for j in range(len(fList)):
                                        framenew[fList[j]] = pd.to_numeric(framenew[fList[j]], errors='coerce')
                                        framenew = framenew.replace(np.nan, 0, regex=True)
                                        add = framenew[fList[j]].astype('float')
                                    print (add.head())
                                    add -= add
                                    print (add.head())
                                col = 'Parch'
                                framenew[col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                num_rows = len(framenew[(add == framenew[col].astype('float'))])
                                oulierIndex = framenew.index[(add != framenew[col].astype('float'))].tolist()

                                r7 = framecopy.loc[oulierIndex]
                                print (r7.head())
                            elif oList[0] == '+':
                                for i in range(iter):
                                    # col = col
                                    # globals()[f"col{i + 1}"] = fList[i]
                                    framenew[fList[i]] = pd.to_numeric(framenew[fList[i]], errors='coerce')
                                    framenew = framenew.replace(np.nan, 0, regex=True)
                                    add = framenew[fList[i]].astype('float')
                                    add += add
                                col = col
                                framenew[col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                num_rows = len(framenew[(add == framenew[col].astype('float'))])
                                oulierIndex = framenew.index[(add != framenew[col].astype('float'))].tolist()

                                r7 = framecopy.loc[oulierIndex]
                            elif oList[0] == '*':
                                for i in range(iter):
                                    # col = col
                                    # globals()[f"col{i + 1}"] = fList[i]
                                    framenew[fList[i]] = pd.to_numeric(framenew[fList[i]], errors='coerce')
                                    framenew = framenew.replace(np.nan, 0, regex=True)
                                    add = framenew[fList[i]].astype('float')
                                    add *= add
                                col = col
                                framenew[col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                num_rows = len(framenew[(add == framenew[col].astype('float'))])
                                oulierIndex = framenew.index[(add != framenew[col].astype('float'))].tolist()

                                r7 = framecopy.loc[oulierIndex]
                            elif oList[0] == '/':
                                for i in range(iter):
                                    # col = col
                                    # globals()[f"col{i + 1}"] = fList[i]
                                    framenew[fList[i]] = pd.to_numeric(framenew[fList[i]], errors='coerce')
                                    framenew = framenew.replace(np.nan, 0, regex=True)
                                    add = framenew[fList[i]].astype('float')
                                    add /= add
                                col = col
                                framenew[col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                num_rows = len(framenew[(add == framenew[col].astype('float'))])
                                oulierIndex = framenew.index[(add != framenew[col].astype('float'))].tolist()

                                r7 = framecopy.loc[oulierIndex]
                            test = ''.join(map(str, itertools.chain.from_iterable(zip(fList, oList))))
                            rulename = str(ru['rule'] + " " + test + str(fList[-1]))
                        refCount=0
                        reftotalCount=0
                        refPerc=0
                        refCount=num_rows
                        #r7.index = r7.index + 2
                        reftotalCount=len(framecopy[col])
                        if reftotalCount!=0:
                            refPerc= (refCount / reftotalCount)*100
                            #Accvalue.append(refPerc)
                        nulls_list=[]
                        if(reftotalCount- refCount>0):
                            DNull = {}
                            DNull['column'] =col
                            DNull['rule'] = rulename
                            DNull['outLiers'] = str(reftotalCount- refCount)


                            count=count+1
                            #resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                            #DNull['outlier']=resultId
                            r7 = r7.where(pd.notnull(r7), 'None')
                            r7.index = r7.index + 2
                            data_dict = r7.to_dict('index')

                            resultsetdict7={}
                            resultsetdict7['resultset'] = 2
                            resultsetdict7['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            #resultsetDictlist.append(resultsetdict7)
                            #AccNulls_list.append(DNull)
    return resultsetdict7


@app.route("/api/checkformulaEditor3", methods=["GET"])
def checkformulaeditor3():
    ru={}
    ru['rule'] ='Formula'
    ru['type']='SIMPLE'
    LISTV=[]
    count=0
    ru['value']=LISTV
    listdict={"cde": "Pclass",
              "operator": "NULL"}
    LISTV.append(listdict)
    listdict1={
              "cde": "SibSp",
              "operator": "-"
            }
    LISTV.append(listdict1)


    col='Age'
    
    rcond=[]
    # rcondl={
    #               "start": "(",
    #               "cde1": "Pclass",
    #               "operator1": "==",
    #               "cde2": "",
    #               "value": "1",
    #               "end": ")",
    #               "condition": "",
    #               "operator2": ""
    #             }
    group1={
              "operand1": "Pclass",
              "operand2": "",
              "operator1": "==",
              "operator2": "",
              "value": "1",
              "condition": "AND",
              "index": "",
              "group": []
            }
    group2={
              "operand1": "Pclass",
              "operand2": "",
              "operator1": "==",
              "operator2": "",
              "value": "2",
              "condition": "OR",
              "index": "",
              "group": []
            }        
    grouplist=[]
    grouplist.append(group1) 
    grouplist.append(group2)       
    rcondl={
          "operand1": "Sex",
          "operator1": "==",
          "operand2":"",
          "value": "male",
          "index": "",
          "group": grouplist,
          "condition":"",
          "operator2": ""

        }
    resultsetdict7={}
    rcond.append(rcondl)
    rl={    "operand": "Age",
            "operator": ">",
            "operand2": "",
            "logic": "If",
            "value": "30",              
            "formula":rcond,
            "formulaText": "Age >30If(Sex==male)"
            
            }
    LISTV1=[]
    LISTV1.append(rl)        
    ru['value']= LISTV1  
    ru["formulastring"]="Age !=(Pclass+29)" 
    framenew= pd.read_csv("testfile.csv")
    if ru['rule'] =='Formula':
                        rulename=""
                        isAccuracy=True
                        formula= ru['value']
                        ifFlag=false
                        formulaText=""
                        for valcol in formula:
                            formulaText=valcol['formulaText'] 
                            if valcol['logic']=="If":
                                ifFlag=true                                
                                break

                        if ifFlag==true:
                            #framenew= framenew.dropna()

                            
                            querystring =GetQueryStringifnew(formula)
                            invertedQString=querystring
                            invertedQString=GetinvertQueryifString(formula)
                            framecopy= framenew.copy(deep=True)
                            print(querystring)
                            print(invertedQString)
                            print(col)
                            print('conditional vala main loop')

                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                print('actual complie with formule: ')
                                print( num_rows)
                                idf2= framenew.query(invertedQString)
                                x = pd.concat([df2, idf2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                
                                num_rows= len(framecopy[col])-len(r7)
                                print('actual count with formule: ')
                                print( num_rows)
                                #print(len(df3))
                                #print((df3.index).tolist())
                            rulename=querystring
                        else:
                            querystring= formulaText#ru['formulaText']
                            formula= ru['value']
                            #querystring =GetQueryStringnew(formula)
                            #invertedQString=GetinvertQueryString(formula)
                            invertedQString=querystring
                            framecopy= framenew.copy(deep=True)
                            print(querystring)
                            print(invertedQString)
                            print(col)
                            print('conditional vala else loop')
                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                print('actual complie with formule: ')
                                print( num_rows)
                                
                                x = pd.concat([framecopy, df2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                num_rows= len(framecopy[col])-len(r7)
                                print('actual count not equla to formule: ')
                                print( num_rows)
                                #print(len(df3))
                                #print((df3.index).tolist())
                            rulename=querystring
                            
                        refCount=0
                        reftotalCount=0
                        refPerc=0
                        refCount=num_rows
                        #r7.index = r7.index + 2
                        reftotalCount=len(framecopy[col])
                        if reftotalCount!=0:
                            refPerc= (refCount / reftotalCount)*100
                            #Accvalue.append(refPerc)
                        nulls_list=[]
                        if(reftotalCount- refCount>0):
                            DNull = {}
                            DNull['column'] =col
                            DNull['rule'] = rulename
                            DNull['outLiers'] = str(reftotalCount- refCount)


                            count=count+1
                            #resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                            #DNull['outlier']=resultId
                            r7 = r7.where(pd.notnull(r7), 'None')
                            r7.index = r7.index + 2
                            data_dict = r7.to_dict('index')

                            resultsetdict7={}
                            resultsetdict7['resultset'] = 2
                            resultsetdict7['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            #resultsetDictlist.append(resultsetdict7)
                            #AccNulls_list.append(DNull)
    return resultsetdict7

@app.route("/api/checkformulaEditor2", methods=["GET"])
def checkformulaeditor2():
    ru={}
    ru['rule'] ='Formula'
    ru['type']='CONDITIONAL1'
    LISTV=[]
    count=0
    ru['value']=LISTV
    listdict={"cde": "Pclass",
              "operator": "NULL"}
    LISTV.append(listdict)
    listdict1={
              "cde": "SibSp",
              "operator": "-"
            }
    LISTV.append(listdict1)


    col='Age'
    
    rcond=[]
    # rcondl={
    #               "start": "(",
    #               "cde1": "Pclass",
    #               "operator1": "==",
    #               "cde2": "",
    #               "value": "1",
    #               "end": ")",
    #               "condition": "",
    #               "operator2": ""
    #             }
    rcondl={
      "operand1": "PassengerId",
      "operator1": "+",
      "operator2": "",
      "value": "23",
      "condition": "",
      "group": []
    }
    rcondl1={
      "operand1": "Pclass",
      "operand2": "",
      "operator1": " ",
      "operator2": "-",
      "value": "",
      "condition": "",
      "group": []
      }
    resultsetdict7={}
    rcond.append(rcondl)
    rcond.append(rcondl1)
    rl={
              "logic": "",              
              "formula":rcond,
              "operand": "Sex",
              "operator": "==",
              "value": ""
            }
    LISTV1=[]
    LISTV1.append(rl)        
    ru['value']= LISTV1  
    ru["formulastring"]="Age >(30)" 
    framenew= pd.read_csv("testfile.csv")
    if ru['rule'] =='Formula':
                        rulename=""
                        isAccuracy=True
                        if ru['type']=='CONDITIONAL':
                            #framenew= framenew.dropna()

                            formula= ru['value']
                            querystring =GetQueryStringnew(formula)
                            invertedQString=GetinvertQueryString(formula)
                            framecopy= framenew.copy(deep=True)
                            print(querystring)
                            print(invertedQString)
                            print(col)
                            print('conditional vala main loop')

                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                print('actual complie with formule: ')
                                print( num_rows)
                                idf2= framenew.query(invertedQString)
                                x = pd.concat([df2, idf2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                print(r7.head())
                                num_rows= len(framecopy[col])-len(r7)
                                print('actual count with formule: ')
                                print( num_rows)
                                #print(len(df3))
                                #print((df3.index).tolist())
                            rulename=querystring
                        else:
                            querystring= ru['formulastring']
                            formula= ru['value']
                            #querystring =GetQueryStringnew(formula)
                            #invertedQString=GetinvertQueryString(formula)
                            invertedQString=querystring
                            framecopy= framenew.copy(deep=True)
                            print(querystring)
                            print(invertedQString)
                            print(col)
                            print('conditional vala else loop')
                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                print('actual complie with formule: ')
                                print( num_rows)
                                
                                x = pd.concat([framenew, df2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                num_rows= len(framenew[col])-len(r7)
                                print('actual count not equla to formule: ')
                                print( num_rows)
                                #print(len(df3))
                                #print((df3.index).tolist())
                            rulename=querystring
                            
                        refCount=0
                        reftotalCount=0
                        refPerc=0
                        refCount=num_rows
                        #r7.index = r7.index + 2
                        reftotalCount=len(framecopy[col])
                        if reftotalCount!=0:
                            refPerc= (refCount / reftotalCount)*100
                            #Accvalue.append(refPerc)
                        nulls_list=[]
                        if(reftotalCount- refCount>0):
                            DNull = {}
                            DNull['column'] =col
                            DNull['rule'] = rulename
                            DNull['outLiers'] = str(reftotalCount- refCount)


                            count=count+1
                            #resultId= (launchNewId)+str(KeyName) +'RS'+str(count)
                            #DNull['outlier']=resultId
                            r7 = r7.where(pd.notnull(r7), 'None')
                            r7.index = r7.index + 2
                            data_dict = r7.to_dict('index')

                            resultsetdict7={}
                            resultsetdict7['resultset'] = 2
                            resultsetdict7['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            #resultsetDictlist.append(resultsetdict7)
                            #AccNulls_list.append(DNull)
    return resultsetdict7

@app.route("/api/checkformula", methods=["GET"])
def checkformula():

    
    column='sex'
    
    rcond=[]
    rcondl={
                  "start": "(",
                  "cde1": "pclass",
                  "operator1": "==",
                  "cde2": "",
                  "value": "1",
                  "end": ")",
                  "condition": "",
                  "operator2": ""
                }
    rcond.append(rcondl)
    rl={
              "logic": "If",
              "conditions":rcond,
              "retcde1": "sex",
              "retoperator": "==",
              "retcde2": "",
              "retvalue": "male"
            }
    df= pd.read_csv("testfile.csv")

    
    if 'Formula' =='Formula':
                        formula= rl
                        querystring=""
                        for valcol in formula:
                            if valcol['logic']=="If":
                                querystring=querystring + '('
                            else:
                                querystring= querystring + '|('
                            querystring= querystring + '('
                            for condn in valcol["conditions"]:
                                    querystring= querystring + condn["cde1"]
                                    querystring= querystring +condn["operator1"]
                                    if(condn["cde2"]!=""):
                                        querystring= querystring + condn["cde2"]
                                    else:
                                        querystring= querystring + "'"+condn["value"] + "'"
                                    if(condn["condition"]!=""):
                                        if(condn["condition"]=="AND"):
                                            querystring= querystring +"&"
                                        elif(condn["condition"]=="OR"):
                                            querystring= querystring +"|"
                                    else:
                                        querystring= querystring + ')'
                            querystring= querystring + '& (' +valcol["retcde1"]
                            querystring= querystring +valcol["retoperator"]
                            if(valcol["retcde2"]!=""):
                                        querystring= querystring + valcol["retcde2"]
                            else:
                                        querystring= querystring +valcol["retvalue"] + ")"
                                        #querystring= querystring + "'"+valcol["retvalue"] + "')"
                            querystring=querystring + ')'
                        framenew= df.copy(deep=True)
                        print(querystring)
                        if querystring!="":
                            num_rows=len(df.query(querystring))
                            print(num_rows)
                            df2=df.query(querystring)
                            print(df2)
                            df3 =df[~df.isin(df2)]
                            print(len(df3))
                            print((df3))

    return 'true'

def getFormatteddf(formula,df):
    querystring=""
    for valcol in formula:
        for condn in valcol["conditions"]:
                if(condn["cde2"]!=""):
                    querystring= querystring + condn["cde2"]
                else:
                    if (condn["value"]).isnumeric():
                        if(condn["cde1"]!=""):
                            df[condn["cde1"]] = pd.to_numeric(df[condn["cde1"]], errors='coerce')
                    else:
                        querystring= querystring + "'"+condn["value"] + "'"
        if(valcol["retcde2"]!=""):
            df=df
        else:
            if (valcol["retvalue"]).isnumeric():
                df[valcol["retcde1"]] = pd.to_numeric(df[valcol["retcde1"]], errors='coerce')
    return df

def GetQueryString(formula):

    querystring=""
    for valcol in formula:
        if valcol['logic']=="If":
            querystring=querystring + '('
        else:
            querystring= querystring + '|('
        querystring= querystring + '('
        for condn in valcol["conditions"]:
                querystring= querystring + condn["cde1"]
                querystring= querystring +condn["operator1"]
                if(condn["cde2"]!=""):
                    querystring= querystring + condn["cde2"]
                else:

                    if (condn["value"]).isnumeric():
                        print(condn["value"])
                        print('numeric')
                        querystring= querystring + condn["value"]
                    else:
                        print(condn["value"])
                        print('non numeric')
                        querystring= querystring + "'"+condn["value"] + "'"
                if(condn["condition"]!=""):
                    if(condn["condition"]=="AND"):
                        querystring= querystring +"&"
                    elif(condn["condition"]=="OR"):
                        querystring= querystring +"|"
                else:
                    querystring= querystring + ')'
        querystring= querystring + '& (' +valcol["retcde1"]
        querystring= querystring +valcol["retoperator"]
        if(valcol["retcde2"]!=""):
                    querystring= querystring + valcol["retcde2"] + ")"
        else:
            if (valcol["retvalue"]).isnumeric():
                    querystring= querystring +valcol["retvalue"] + ")"
            else:
                    querystring= querystring + "'"+valcol["retvalue"] + "')"
        querystring=querystring + ')'

    return querystring


def GetQueryStringifnew(formula):

    querystring=""
    for valcol in formula:
        if valcol['logic']=="If":
            querystring=querystring + '('
        else:
            querystring= querystring + '|('
        querystring= querystring + '('
        for condn in valcol["formula"]:
                querystring= querystring + condn["operand1"]
                querystring= querystring +condn["operator1"]
                if ( condn["operand2"]!=""):
                    querystring= querystring + condn["operand2"]
                else:

                    if (condn["value"]).isnumeric():
                        print(condn["value"])
                        print('numeric')
                        querystring= querystring + condn["value"]
                    else:
                        print(condn["value"])
                        print('non numeric')
                        querystring= querystring + "'"+condn["value"] + "'"
                if( condn["condition"]!=""):
                    if(condn["condition"]=="AND"):
                        querystring= querystring +"&"
                    elif(condn["condition"]=="OR"):
                        querystring= querystring +"|"
                else:
                    #print(condn["operator2"])
                    if ( condn["operator2"]!=""):

                        querystring= querystring + condn["operator2"]
                    else:
                        querystring= querystring + ')'
                if( condn["group"]):
                        querystringg= GetQueryStringGroupnew(formula)
                        print(querystringg)
                        querystring= querystring +querystringg        
        querystring= querystring + '& (' +valcol["operand"]
        querystring= querystring +valcol["operator"]
        if (1!=1):# if(valcol["operand2"]!=""):
                    querystring= querystring + valcol["operand2"] + ")"
        else:
            if (valcol["value"]).isnumeric():
                    querystring= querystring +valcol["value"] + ")"
            else:
                    querystring= querystring + "'"+valcol["value"] + "')"
                    
        querystring=querystring + ')'

    return querystring

def GetQueryStringGroupnew(formula):

    querystring=""
    i=0
    for valcol in formula:        
        
        for condn in valcol["formula"]:
                listGroups= condn['group']

                for listGroup in listGroups:
                    i=i+1    
                    if( listGroup["condition"]!=""):
                            if(listGroup["condition"]=="AND"):
                                if (i==1):
                                     querystring= querystring +" & ( "
                                else:
                                    querystring= querystring +" & "
                            elif(listGroup["condition"]=="OR"):
                                if (i==1):
                                     querystring= querystring +" | ( "
                                else:
                                    querystring= querystring +" | "
                    else:
                            if ( listGroup["operator2"]!=""):
                                querystring= querystring + listGroup["operator2"]
                            else:
                                querystring= querystring + ')'
                    querystring= querystring + '('            
                
                    querystring= querystring + listGroup["operand1"]
                    querystring= querystring +listGroup["operator1"]
                    if ( condn["operand2"]!=""):
                        querystring= querystring + listGroup["operand2"]
                    else:

                        if (listGroup["value"]).isnumeric():
                            print(listGroup["value"])
                            print('numeric')
                            querystring= querystring + listGroup["value"]
                        else:
                            print(listGroup["value"])
                            print('non numeric')
                            querystring= querystring + "'"+listGroup["value"] + "'"
                    querystring=querystring + ')'                
        querystring=querystring + ')'
    return querystring

def GetQueryStringnew(formula):

    querystring=""
    for valcol in formula:
        if valcol['logic']=="If":
            querystring=querystring + '('
        else:
            querystring= querystring + '|('
        querystring= querystring + '('
        for condn in valcol["conditions"]:
                querystring= querystring + condn["cde1"]
                querystring= querystring +condn["operator1"]
                if(condn["cde2"]!=""):
                    querystring= querystring + condn["cde2"]
                else:

                    if (condn["value"]).isnumeric():
                        print(condn["value"])
                        print('numeric')
                        querystring= querystring + condn["value"]
                    else:
                        print(condn["value"])
                        print('non numeric')
                        querystring= querystring + "'"+condn["value"] + "'"
                if(condn["condition"]!=""):
                    if(condn["condition"]=="AND"):
                        querystring= querystring +"&"
                    elif(condn["condition"]=="OR"):
                        querystring= querystring +"|"
                else:
                    print(condn["operator2"])
                    if (condn["operator2"]!=""):

                        querystring= querystring + condn["operator2"]
                    else:
                        querystring= querystring + ')'
        querystring= querystring + '& (' +valcol["retcde1"]
        querystring= querystring +valcol["retoperator"]
        if(valcol["retcde2"]!=""):
                    querystring= querystring + valcol["retcde2"] + ")"
        else:
            if (valcol["retvalue"]).isnumeric():
                    querystring= querystring +valcol["retvalue"] + ")"
            else:
                    querystring= querystring + "'"+valcol["retvalue"] + "')"
        querystring=querystring + ')'

    return querystring
def GetQueryStringv1(formula):

    querystring=""
    for valcol in formula:
        if valcol['logic']=="If":
            querystring=querystring + '('
        else:
            querystring= querystring + '|('
        querystring= querystring + '('
        for condn in valcol["conditions"]:
                querystring= querystring + condn["operand1"]
                querystring= querystring +condn["operator1"]
                if(condn["operand2"]!=""):
                    querystring= querystring + condn["operand2"]
                else:

                    if (condn["value"]).isnumeric():
                        print(condn["value"])
                        print('numeric')
                        querystring= querystring + condn["value"]
                    else:
                        print(condn["value"])
                        print('non numeric')
                        querystring= querystring + "'"+condn["value"] + "'"
                if(condn["condition"]!=""):
                    if(condn["condition"]=="AND"):
                        querystring= querystring +"&"
                    elif(condn["condition"]=="OR"):
                        querystring= querystring +"|"
                else:
                    print(condn["operator2"])
                    if (condn["operator2"]!=""):

                        querystring= querystring + condn["operator2"]
                    else:
                        querystring= querystring + ')'
        querystring= querystring + '& (' +valcol["operand1"]
        querystring= querystring +valcol["operator"]
        if(valcol["operand2"]!=""):
                    querystring= querystring + valcol["operand2"] + ")"
        else:
            if (valcol["value"]).isnumeric():
                    querystring= querystring +valcol["value"] + ")"
            else:
                    querystring= querystring + "'"+valcol["value"] + "')"
        querystring=querystring + ')'

    return querystring

def GetinvertQueryString(formula):

    querystring=""
    for valcol in formula:
        if valcol['logic']=="If":
            querystring=querystring + '('
        else:
            querystring= querystring + '|('
        querystring= querystring + '('
        for condn in valcol["conditions"]:
                querystring= querystring + condn["cde1"]
                querystring= querystring +condn["operator1"]
                if(condn["cde2"]!=""):
                    querystring= querystring + condn["cde2"]
                else:
                    if (condn["value"]).isnumeric():
                        print(condn["value"])
                        print('numeric')
                        querystring= querystring + condn["value"]
                    else:
                        print(condn["value"])
                        print('non numeric')
                        querystring= querystring + "'"+condn["value"] + "'"
                if(condn["condition"]!=""):
                    if(condn["condition"]=="AND"):
                        querystring= querystring +"&"
                    elif(condn["condition"]=="OR"):
                        querystring= querystring +"|"
                else:
                    if (condn["operator2"]!=""):

                        querystring= querystring + condn["operator2"]
                    else:
                        querystring= querystring + ')'
        querystring=querystring + ')'

    return querystring

def GetinvertQueryifString(formula):

    querystring=""
    for valcol in formula:
        if valcol['logic']=="If":
            querystring=querystring + '('
        else:
            querystring= querystring + '|('
        querystring= querystring + '('
        for condn in valcol["formula"]:
                querystring= querystring + condn["operand1"]
                querystring= querystring +condn["operator1"]
                if( condn["operand2"]!=""):
                    querystring= querystring + condn["operand2"]
                else:

                    if (condn["value"]).isnumeric():
                        print(condn["value"])
                        print('numeric')
                        querystring= querystring + condn["value"]
                    else:
                        print(condn["value"])
                        print('non numeric')
                        querystring= querystring + "'"+condn["value"] + "'"
                if( condn["condition"]!=""):
                    if(condn["condition"]=="AND"):
                        querystring= querystring +"&"
                    elif(condn["condition"]=="OR"):
                        querystring= querystring +"|"
                else:
                    #print(condn["operator2"])
                    if ( condn["operator2"]!=""):

                        querystring= querystring + condn["operator2"]
                    else:
                        querystring= querystring + ')'
        querystring=querystring + ')'

    return querystring


@app.route('/api/rules_mdm', methods=['POST']) #GET requests will be blocked
def rules_mdm():
    content = request.get_json()
    results=getrules_mdm(content)
    return results

@app.route('/api/rules_mdm_global', methods=['POST']) #GET requests will be blocked
def rules_mdmglobal():
    content = request.get_json()
    results=getrules_mdmglobal(content)
    return results

@app.route('/api/rules', methods=['POST']) #GET requests will be blocked
def rulesWithRefthreadnew():
    content = request.get_json()
    sourcepath= content['sourcepath']
    connectiondetails={}
    typeofcon=""
    if "type" in content:
        if content["type"]== "oracle":
            connContent=content["connectionDetails"]
            connectiondetails= content["connectionDetails"]
            typeofcon = "oracle"
            host=connContent['host']
            dbName = connContent['dbName']
            userName = connContent['username']
            port = connContent['port']
            password = connContent['password']
            table = connContent['sourceTableName']
            df = getdfFromTable(host,dbName,port,userName,password,table)
    else:
        df= df_from_pathwithoutTimeConversion(sourcepath)

    # 06/06/2022- Thiru #78 
    # Convert all the datetime columns to Datetime data type
    df = force_datetime_conversion(df)

    rules=[]
    selectedColumns= content['selectedColumns']
    df_meta = pd.DataFrame(df.dtypes).reset_index()
    df_meta.columns = ["column_name", "dtype"]
    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")

    conFeat = df_meta[df_meta.dtype == "int64"].column_name.tolist() + df_meta[df_meta.dtype == "float64"].column_name.tolist()
    catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
    dateFeat = df_meta[df_meta.dtype == "datetime64[ns]"].column_name.tolist() + df_meta[df_meta.dtype == "datetime"].column_name.tolist()

    if(len(conFeat)>0):
        #NumericCorrMatrix=GetCorrelationMatrix(sourcepath, 'numeric','pearson')
        if typeofcon == "oracle":
            NumericCorrMatrix=GetCorrelationMatrix_v1(df, 'numeric','pearson',"oracle",connectiondetails )
        else:
            NumericCorrMatrix=GetCorrelationMatrix(df, 'numeric','pearson')
    if(len(catFeat)>0):
        #CateoricalCorrMatrix=GetCorrelationMatrix(sourcepath, 'categorical' ,'theils_u')
        if typeofcon == "oracle":
            CateoricalCorrMatrix=GetCorrelationMatrix_v1(df, 'categorical' ,'theils_u',"oracle",connectiondetails )
        else:
            CateoricalCorrMatrix=GetCorrelationMatrix(df, 'categorical' ,'theils_u')

    refColumns= content['refSelectedColumns']

    # 06/06/2022- Thiru #51 & #57 (Ruleset formula detection issues)
    # Create new list to hold all the features - Upper / Lower triangle of correlation matrix 
    df_clean = df.select_dtypes(exclude=['object','datetime64[ns]'])
    # df_clean = df_clean.loc[:,df_clean.apply(pd.Series.nunique) != 1]

    corr_mat = df_clean.corr(method='spearman')
    upper_corr_mat = corr_mat.where(np.triu(np.ones(corr_mat.shape), k=1).astype(bool))
    response_vars_upp = list(upper_corr_mat.columns)
    unique_corr_pairs_upper = upper_corr_mat.unstack().dropna()

    lower_corr_mat = corr_mat.where(np.tril(np.ones(corr_mat.shape), k=1).astype(bool))
    response_vars_low = list(lower_corr_mat.columns)
    unique_corr_pairs_lower = lower_corr_mat.unstack().dropna()

    unique_corr_pairs = pd.concat([unique_corr_pairs_upper, unique_corr_pairs_lower], axis=0)

    sorted_mat   = unique_corr_pairs.sort_values(ascending=False)
    df_corr_sort = pd.DataFrame(data = sorted_mat, index = sorted_mat.index, columns = ['Corr_Coeff'])

    q = mp.Queue()
    jobs = []
    i=0
    # 06/06/2022- Thiru #51 & #57 (Ruleset formula detection issues)
    for k in selectedColumns:
        p = mp.Process(target=rulesforSingleColumn, args=(df,k,conFeat,catFeat,dateFeat,NumericCorrMatrix,CateoricalCorrMatrix,refColumns, df_corr_sort,q))
        jobs.append(p)
        p.start()
        print('starting')
    print('completed')
    results = [q.get() for j in jobs]
    print(results)
    json_data = json.dumps(results, default=str)
    
    # for k in selectedColumns:
    #     print('starting')
    #     Dict= rulesforSingleColumnTest(df,k,conFeat,catFeat,dateFeat,NumericCorrMatrix,CateoricalCorrMatrix,refColumns, df_corr_sort)
    #     rules.append(Dict)
    # print('completed')
    # json_data = json.dumps(rules, default=str)

    return json_data



@app.route('/api/rulestest', methods=['GET']) #GET requests will be blocked
def rulesWithtest():
    
    sourcepath= 'S1Titanic.xls'
    connectiondetails={}
    typeofcon=""
    df= df_from_pathwithoutTimeConversion(sourcepath)

    # 06/06/2022- Thiru #78 
    # Convert all the datetime columns to Datetime data type
    df = force_datetime_conversion(df)

    rules=[]
    selectedColumns= ['PassengerId', 'Pclass', 'Age', 'SibSp', 'Parch', 'Fare', 'Name', 'Sex', 'Ticket', 'Cabin', 'Embarked']
    df_meta = pd.DataFrame(df.dtypes).reset_index()
    df_meta.columns = ["column_name", "dtype"]
    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")

    conFeat = df_meta[df_meta.dtype == "int64"].column_name.tolist() + df_meta[df_meta.dtype == "float64"].column_name.tolist()
    catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
    dateFeat = df_meta[df_meta.dtype == "datetime64[ns]"].column_name.tolist() + df_meta[df_meta.dtype == "datetime"].column_name.tolist()

    if(len(conFeat)>0):
        #NumericCorrMatrix=GetCorrelationMatrix(sourcepath, 'numeric','pearson')
        if typeofcon == "oracle":
            NumericCorrMatrix=GetCorrelationMatrix_v1(df, 'numeric','pearson',"oracle",connectiondetails )
        else:
            NumericCorrMatrix=GetCorrelationMatrix(df, 'numeric','pearson')
    if(len(catFeat)>0):
        #CateoricalCorrMatrix=GetCorrelationMatrix(sourcepath, 'categorical' ,'theils_u')
        if typeofcon == "oracle":
            CateoricalCorrMatrix=GetCorrelationMatrix_v1(df, 'categorical' ,'theils_u',"oracle",connectiondetails )
        else:
            CateoricalCorrMatrix=GetCorrelationMatrix(df, 'categorical' ,'theils_u')

    refColumns=[]#content['refSelectedColumns']

    # 06/06/2022- Thiru #51 & #57 (Ruleset formula detection issues)
    # Create new list to hold all the features - Upper / Lower triangle of correlation matrix 
    df_clean = df.select_dtypes(exclude=['object','datetime64[ns]'])
    # df_clean = df_clean.loc[:,df_clean.apply(pd.Series.nunique) != 1]

    corr_mat = df_clean.corr(method='spearman')
    upper_corr_mat = corr_mat.where(np.triu(np.ones(corr_mat.shape), k=1).astype(bool))
    response_vars_upp = list(upper_corr_mat.columns)
    unique_corr_pairs_upper = upper_corr_mat.unstack().dropna()

    lower_corr_mat = corr_mat.where(np.tril(np.ones(corr_mat.shape), k=1).astype(bool))
    response_vars_low = list(lower_corr_mat.columns)
    unique_corr_pairs_lower = lower_corr_mat.unstack().dropna()

    unique_corr_pairs = pd.concat([unique_corr_pairs_upper, unique_corr_pairs_lower], axis=0)

    sorted_mat   = unique_corr_pairs.sort_values(ascending=False)
    df_corr_sort = pd.DataFrame(data = sorted_mat, index = sorted_mat.index, columns = ['Corr_Coeff'])

    q = mp.Queue()
    jobs = []
    i=0
    # 06/06/2022- Thiru #51 & #57 (Ruleset formula detection issues)
    results=[]
    print(results)
    json_data = json.dumps(results, default=str)
    
    # for k in selectedColumns:
    #     print('starting')
    #     Dict= rulesforSingleColumnTest(df,k,conFeat,catFeat,dateFeat,NumericCorrMatrix,CateoricalCorrMatrix,refColumns, df_corr_sort)
    #     rules.append(Dict)
    # print('completed')
    # json_data = json.dumps(rules, default=str)

    return json_data

def count_digits(string):
    return sum(item.isdigit() for item in string)

def rulesforSingleColumn(df,selectedColumn,conFeat,catFeat,dateFeat,NumericCorrMatrix,CateoricalCorrMatrix,refColumns, df_corr_sort,q):
    content = request.get_json()
    sourcepath= content['sourcepath']
    connectiondetails={}
    typeofcon=""
    rules=[]
    ruleConfigfor2Cols = content["rulesfor2cols"]
    ruleConfigfor3Cols = content["rulesfor3cols"]
    ruleConfigforMultiCols = content["rulesformulticols"]

    if (ruleConfigforMultiCols == "") and (len(ruleConfigforMultiCols) < 3):
        return {'errorMsg' : "Atleast 3 columns should be selected for creating formula or ruleset!", "errorCode" : '110', 'errorflag': 'True' }


    Dict = {}
    try:
        if selectedColumn!="":
            k=selectedColumn
            statisticAnal= []

            if k in conFeat:
                df_describe_continuous = processContinuous([k], df)
                Dict['statistics'] =df_describe_continuous
                
                try:
                    ResMatrix=sorted_corr_matrix_per_col(NumericCorrMatrix,k)
                    Dict['correlationSummary']=ResMatrix    
                except:
                    Dict['correlationSummary']={}
                statisticAnal=df_describe_continuous
            if k in catFeat:
                df_describe_categorical = processCategorical([k], df)
                Dict['statistics'] =df_describe_categorical
                try:
                    ResMatrix=sorted_corr_matrix_per_col(CateoricalCorrMatrix,k)
                    Dict['correlationSummary']=ResMatrix    
                except:
                    Dict['correlationSummary']={}
                statisticAnal=df_describe_categorical

            # 06/06/2022- Thiru #78 (New Proc to handle the date columns)
            if k in dateFeat:
                df_describe_datetime = processDatetime([k], df)
                Dict['statistics'] =df_describe_datetime
                Dict['correlationSummary']={}
                statisticAnal=df_describe_datetime

            corrMatrix={}
            RulesDict = {}
            rules_list=[]
            if is_string_dtype(df[k]):
                RulesDict['rule'] ='DataType'
                isAlphacount = df[k].str.isalpha().sum()
                isAlphaNumcount= df[k].str.isalnum().sum() - isAlphacount

                for aval in statisticAnal:
                    Dict['DataType']=aval['suggested_dtype']
                    RulesDict['value']=aval['suggested_dtype']
                    if (aval['suggested_dtype']=="Alphabetic"):
                        Dict['DataType']='Text'
                        RulesDict['value'] =  'Text'


                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'
                RulesDict['format']  = ''
                rules_list.append(RulesDict)
                if (aval['suggested_dtype']!="Numeric"):
                    minLength= df[k].str.len().min()
                    maxLength=df[k].str.len().max()
                    if (minLength)== (maxLength):
                        RulesDict1 = {}
                        RulesDict1['rule'] ='Length'
                        RulesDict1['value'] = str(maxLength)
                        RulesDict1['dimension'] =  'Validity'
                        RulesDict1['operator']  = 'equalto'
                        RulesDict1['format']  = ''
                        rules_list.append(RulesDict1)
                    else:
                        MinRulesDict1 = {}
                        MinRulesDict1['rule'] ='MaxLength'
                        MinRulesDict1['value'] = str(maxLength)
                        MinRulesDict1['dimension'] =  'Validity'
                        MinRulesDict1['operator']  = 'equalto'
                        MinRulesDict1['format']  = ''
                        rules_list.append(MinRulesDict1)

                        MaxRulesDict1 = {}
                        MaxRulesDict1['rule'] ='MinLength'
                        MaxRulesDict1['value'] = str(minLength)
                        MaxRulesDict1['dimension'] =  'Validity'
                        MaxRulesDict1['operator']  = 'equalto'
                        MaxRulesDict1['format']  = ''
                        rules_list.append(MaxRulesDict1)
                     

            if is_numeric_dtype(df[k]):
                RulesDict['rule'] ='DataType'
                Dict['DataType']='Numeric'
                RulesDict['value'] =  'Numeric'
                for aval in statisticAnal:
                    RulesDict['value']=aval['suggested_dtype']
                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'
                RulesDict['format']  = ''
                rules_list.append(RulesDict)
                
                df['counts'] = df[k].astype(str).apply(count_digits)
                minLength= df['counts'].min()
                maxLength=df['counts'].max()
                if (minLength)== (maxLength):
                    RulesDict1 = {}
                    RulesDict1['rule'] ='Length'
                    RulesDict1['value'] = str(maxLength)
                    RulesDict1['dimension'] =  'Validity'
                    RulesDict1['operator']  = 'equalto'
                    RulesDict1['format']  = ''
                    rules_list.append(RulesDict1)
                else:
                    MinRulesDict1 = {}
                    MinRulesDict1['rule'] ='MaxLength'
                    MinRulesDict1['value'] = str(maxLength)
                    MinRulesDict1['dimension'] =  'Validity'
                    MinRulesDict1['operator']  = 'equalto'
                    MinRulesDict1['format']  = ''
                    rules_list.append(MinRulesDict1)

                    MaxRulesDict1 = {}
                    MaxRulesDict1['rule'] ='MinLength'
                    MaxRulesDict1['value'] = str(minLength)
                    MaxRulesDict1['dimension'] =  'Validity'
                    MaxRulesDict1['operator']  = 'equalto'
                    MaxRulesDict1['format']  = ''
                    rules_list.append(MaxRulesDict1)
                print(df[k].nunique())
                
                # 06/06/2022- Thiru #51 & #57 (Ruleset formula detection issues)
                # Retrieve all the features to generate feature combinations for finding rules
                # Ruleset creation process will be executed only when the feature has more than one category/value
                # If the feature has one unique value, the correlation will fail (df_corr_sort will not have this feature)
                if df[k].nunique() > 1 :
                    df_respvar1 = df_corr_sort.loc[k].sort_values(by = ['Corr_Coeff'], ascending=False)
                    # dependent_features = list(set(df_respvar1.index))
                    dependent_features = list(dict.fromkeys(df_respvar1.index))
                    dependent_features.remove(k)

                    # Find relationship between 2 columns (Simple)
                    if ruleConfigfor2Cols != "":
                        if ruleConfigfor2Cols == "Sample":
                            df_rule = df.head(1000)
                        elif ruleConfigfor2Cols == "Full":
                            df_rule = df.copy()
                        nr_combinations = 2
                        dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
                        add_new_rule = getRelationshipForRules(df_rule, k, nr_combinations, dep_vars_pairwise_combinations)
                        if add_new_rule:
                            rules_list.append(add_new_rule)
                    
                    # Find relationship between 3 columns
                    if ruleConfigfor3Cols != "":
                        if ruleConfigfor3Cols == "Sample":
                            df_rule = df.head(1000)
                        elif ruleConfigfor3Cols == "Full":
                            df_rule = df.copy()
                        nr_combinations = 3
                        dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
                        add_new_rule = getRelationshipForRules(df_rule, k, nr_combinations, dep_vars_pairwise_combinations)
                        if add_new_rule:
                            rules_list.append(add_new_rule)
                    
                    # Find relationship between multi (selected) columns
                    if len(ruleConfigforMultiCols) > 0:
                        dependent_features = content["rulesformulticols"]
                        if k in dependent_features:
                            df_rule = df[dependent_features]
                            for i in range(2, len(dependent_features)):
                                nr_combinations = i
                                dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
                                add_new_rule = getRelationshipForRules(df_rule, k, nr_combinations, dep_vars_pairwise_combinations)
                                if add_new_rule:
                                    rules_list.append(add_new_rule)

            # 06/06/2022- Thiru #78 (New process to handle the date columns)
            if is_datetime64_any_dtype(df[k]):
                RulesDict['rule'] ='DataType'
                Dict['DataType']='Datetime'
                RulesDict['value'] =  'Datetime'
                for aval in statisticAnal:
                    RulesDict['value']=aval['suggested_dtype']
                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'
                RulesDict['format']  = ''
                rules_list.append(RulesDict)
                
                df['counts'] = df[k].astype(str).apply(count_digits)
                minLength= df['counts'].min()
                maxLength=df['counts'].max()
                if (minLength)== (maxLength):
                    RulesDict1 = {}
                    RulesDict1['rule'] ='Length'
                    RulesDict1['value'] = str(maxLength)
                    RulesDict1['dimension'] =  'Validity'
                    RulesDict1['operator']  = 'equalto'
                    RulesDict1['format']  = ''
                    rules_list.append(RulesDict1)
                else:
                    MinRulesDict1 = {}
                    MinRulesDict1['rule'] ='MaxLength'
                    MinRulesDict1['value'] = str(maxLength)
                    MinRulesDict1['dimension'] =  'Validity'
                    MinRulesDict1['operator']  = 'equalto'
                    MinRulesDict1['format']  = ''
                    rules_list.append(MinRulesDict1)

                    MaxRulesDict1 = {}
                    MaxRulesDict1['rule'] ='MinLength'
                    MaxRulesDict1['value'] = str(minLength)
                    MaxRulesDict1['dimension'] =  'Validity'
                    MaxRulesDict1['operator']  = 'equalto'
                    MaxRulesDict1['format']  = ''
                    rules_list.append(MaxRulesDict1)
                print(df[k].nunique())

            if any(k in s for s in refColumns):
                RulesDict2 = {}
                RulesDict2['rule'] ='ReferenceCDE'
                matchers = []
                matchers.append(k)
                matching = [i for i in refColumns if k in i]
                RulesDict2['value'] =  (list(matching))[0]
                RulesDict2['dimension'] =  'Integrity'
                RulesDict2['operator']  = 'Shouldbe'
                RulesDict2['format']  = ''
                rules_list.append(RulesDict2)
                print(str(matching))

            if (df[k].nunique()!=0):
                Dict['column'] =k
                Dict['rules'] = rules_list
                rules.append(Dict)
            else:
                Dict['column'] =k
                Dict['rules'] = rules_list
                Dict['statistics']=[]
                rules.append(Dict)
        # return Dict
        q.put(Dict)
    except Exception as e:
        print(str(e))
        q.put(Dict)

def rulesforSingleColumnTest(df,selectedColumn,conFeat,catFeat,dateFeat,NumericCorrMatrix,CateoricalCorrMatrix,refColumns, df_corr_sort):
    content = request.get_json()
    sourcepath= content['sourcepath']
    connectiondetails={}
    typeofcon=""
    rules=[]
    ruleConfigfor2Cols = content["rulesfor2cols"]
    ruleConfigfor3Cols = content["rulesfor3cols"]
    ruleConfigforMultiCols = content["rulesformulticols"]

    if (ruleConfigforMultiCols == "") and (len(ruleConfigforMultiCols) < 3):
        return {'errorMsg' : "Atleast 3 columns should be selected for creating formula or ruleset!", "errorCode" : '110', 'errorflag': 'True' }


    Dict = {}
    try:
        if selectedColumn!="":
            k=selectedColumn
            statisticAnal= []

            if k in conFeat:
                df_describe_continuous = processContinuous([k], df)
                Dict['statistics'] =df_describe_continuous
                
                try:
                    ResMatrix=sorted_corr_matrix_per_col(NumericCorrMatrix,k)
                    Dict['correlationSummary']=ResMatrix    
                except:
                    Dict['correlationSummary']={}
                statisticAnal=df_describe_continuous
            if k in catFeat:
                df_describe_categorical = processCategorical([k], df)
                Dict['statistics'] =df_describe_categorical
                try:
                    ResMatrix=sorted_corr_matrix_per_col(CateoricalCorrMatrix,k)
                    Dict['correlationSummary']=ResMatrix    
                except:
                    Dict['correlationSummary']={}
                statisticAnal=df_describe_categorical

            # 06/06/2022- Thiru #78 (New Proc to handle the date columns)
            if k in dateFeat:
                df_describe_datetime = processDatetime([k], df)
                Dict['statistics'] =df_describe_datetime
                Dict['correlationSummary']={}
                statisticAnal=df_describe_datetime

            corrMatrix={}
            RulesDict = {}
            rules_list=[]
            if is_string_dtype(df[k]):
                RulesDict['rule'] ='DataType'
                isAlphacount = df[k].str.isalpha().sum()
                isAlphaNumcount= df[k].str.isalnum().sum() - isAlphacount

                for aval in statisticAnal:
                    Dict['DataType']=aval['suggested_dtype']
                    RulesDict['value']=aval['suggested_dtype']
                    if (aval['suggested_dtype']=="Alphabetic"):
                        Dict['DataType']='Text'
                        RulesDict['value'] =  'Text'


                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'
                RulesDict['format']  = ''
                rules_list.append(RulesDict)
                if (aval['suggested_dtype']!="Numeric"):
                    minLength= df[k].str.len().min()
                    maxLength=df[k].str.len().max()
                    if (minLength)== (maxLength):
                        RulesDict1 = {}
                        RulesDict1['rule'] ='Length'
                        RulesDict1['value'] = str(maxLength)
                        RulesDict1['dimension'] =  'Validity'
                        RulesDict1['operator']  = 'equalto'
                        RulesDict1['format']  = ''
                        rules_list.append(RulesDict1)
                    else:
                        MinRulesDict1 = {}
                        MinRulesDict1['rule'] ='MaxLength'
                        MinRulesDict1['value'] = str(maxLength)
                        MinRulesDict1['dimension'] =  'Validity'
                        MinRulesDict1['operator']  = 'equalto'
                        MinRulesDict1['format']  = ''
                        rules_list.append(MinRulesDict1)

                        MaxRulesDict1 = {}
                        MaxRulesDict1['rule'] ='MinLength'
                        MaxRulesDict1['value'] = str(minLength)
                        MaxRulesDict1['dimension'] =  'Validity'
                        MaxRulesDict1['operator']  = 'equalto'
                        MaxRulesDict1['format']  = ''
                        rules_list.append(MaxRulesDict1)
                     

            if is_numeric_dtype(df[k]):
                RulesDict['rule'] ='DataType'
                Dict['DataType']='Numeric'
                RulesDict['value'] =  'Numeric'
                for aval in statisticAnal:
                    RulesDict['value']=aval['suggested_dtype']
                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'
                RulesDict['format']  = ''
                rules_list.append(RulesDict)
                
                df['counts'] = df[k].astype(str).apply(count_digits)
                minLength= df['counts'].min()
                maxLength=df['counts'].max()
                if (minLength)== (maxLength):
                    RulesDict1 = {}
                    RulesDict1['rule'] ='Length'
                    RulesDict1['value'] = str(maxLength)
                    RulesDict1['dimension'] =  'Validity'
                    RulesDict1['operator']  = 'equalto'
                    RulesDict1['format']  = ''
                    rules_list.append(RulesDict1)
                else:
                    MinRulesDict1 = {}
                    MinRulesDict1['rule'] ='MaxLength'
                    MinRulesDict1['value'] = str(maxLength)
                    MinRulesDict1['dimension'] =  'Validity'
                    MinRulesDict1['operator']  = 'equalto'
                    MinRulesDict1['format']  = ''
                    rules_list.append(MinRulesDict1)

                    MaxRulesDict1 = {}
                    MaxRulesDict1['rule'] ='MinLength'
                    MaxRulesDict1['value'] = str(minLength)
                    MaxRulesDict1['dimension'] =  'Validity'
                    MaxRulesDict1['operator']  = 'equalto'
                    MaxRulesDict1['format']  = ''
                    rules_list.append(MaxRulesDict1)
                print(df[k].nunique())
                
                # 06/06/2022- Thiru #51 & #57 (Ruleset formula detection issues)
                # Retrieve all the features to generate feature combinations for finding rules
                # Ruleset creation process will be executed only when the feature has more than one category/value
                # If the feature has one unique value, the correlation will fail (df_corr_sort will not have this feature)
                if df[k].nunique() > 1 :
                    df_respvar1 = df_corr_sort.loc[k].sort_values(by = ['Corr_Coeff'], ascending=False)
                    # dependent_features = list(set(df_respvar1.index))
                    dependent_features = list(dict.fromkeys(df_respvar1.index))
                    dependent_features.remove(k)

                    # Find relationship between 2 columns (Simple)
                    if ruleConfigfor2Cols != "":
                        if ruleConfigfor2Cols == "Sample":
                            df_rule = df.head(50)
                        elif ruleConfigfor2Cols == "Full":
                            df_rule = df.copy()
                        nr_combinations = 2
                        dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
                        add_new_rule = getRelationshipForRules(df_rule, k, nr_combinations, dep_vars_pairwise_combinations)
                        if add_new_rule:
                            rules_list.append(add_new_rule)
                    
                    # Find relationship between 3 columns
                    if ruleConfigfor3Cols != "":
                        if ruleConfigfor3Cols == "Sample":
                            df_rule = df.head(50)
                        elif ruleConfigfor3Cols == "Full":
                            df_rule = df.copy()
                        nr_combinations = 3
                        dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
                        add_new_rule = getRelationshipForRules(df_rule, k, nr_combinations, dep_vars_pairwise_combinations)
                        if add_new_rule:
                            rules_list.append(add_new_rule)
                    
                    # Find relationship between multi (selected) columns
                    if len(ruleConfigforMultiCols) > 0:
                        dependent_features = content["rulesformulticols"]
                        if k in dependent_features:
                            df_rule = df[dependent_features]
                            for i in range(2, len(dependent_features)):
                                nr_combinations = i
                                dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
                                add_new_rule = getRelationshipForRules(df_rule, k, nr_combinations, dep_vars_pairwise_combinations)
                                if add_new_rule:
                                    rules_list.append(add_new_rule)

            # 06/06/2022- Thiru #78 (New process to handle the date columns)
            if is_datetime64_any_dtype(df[k]):
                RulesDict['rule'] ='DataType'
                Dict['DataType']='Datetime'
                RulesDict['value'] =  'Datetime'
                for aval in statisticAnal:
                    RulesDict['value']=aval['suggested_dtype']
                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'
                RulesDict['format']  = ''
                rules_list.append(RulesDict)
                
                df['counts'] = df[k].astype(str).apply(count_digits)
                minLength= df['counts'].min()
                maxLength=df['counts'].max()
                if (minLength)== (maxLength):
                    RulesDict1 = {}
                    RulesDict1['rule'] ='Length'
                    RulesDict1['value'] = str(maxLength)
                    RulesDict1['dimension'] =  'Validity'
                    RulesDict1['operator']  = 'equalto'
                    RulesDict1['format']  = ''
                    rules_list.append(RulesDict1)
                else:
                    MinRulesDict1 = {}
                    MinRulesDict1['rule'] ='MaxLength'
                    MinRulesDict1['value'] = str(maxLength)
                    MinRulesDict1['dimension'] =  'Validity'
                    MinRulesDict1['operator']  = 'equalto'
                    MinRulesDict1['format']  = ''
                    rules_list.append(MinRulesDict1)

                    MaxRulesDict1 = {}
                    MaxRulesDict1['rule'] ='MinLength'
                    MaxRulesDict1['value'] = str(minLength)
                    MaxRulesDict1['dimension'] =  'Validity'
                    MaxRulesDict1['operator']  = 'equalto'
                    MaxRulesDict1['format']  = ''
                    rules_list.append(MaxRulesDict1)
                print(df[k].nunique())

            if any(k in s for s in refColumns):
                RulesDict2 = {}
                RulesDict2['rule'] ='ReferenceCDE'
                matchers = []
                matchers.append(k)
                matching = [i for i in refColumns if k in i]
                RulesDict2['value'] =  (list(matching))[0]
                RulesDict2['dimension'] =  'Integrity'
                RulesDict2['operator']  = 'Shouldbe'
                RulesDict2['format']  = ''
                rules_list.append(RulesDict2)
                print(str(matching))

            if (df[k].nunique()!=0):
                Dict['column'] =k
                Dict['rules'] = rules_list
                rules.append(Dict)
            else:
                Dict['column'] =k
                Dict['rules'] = rules_list
                Dict['statistics']=[]
                rules.append(Dict)
        return Dict
        # q.put(Dict)
    except Exception as e:
        print(str(e))
        # q.put(Dict)

def getRelationshipForRules(df_rule, k, nr_combinations, dep_vars_pairwise_combinations):
    corrMatrix = {}
    RulesDictf={}
    if df_rule[k].nunique()>10:
        print('call correlaioships')
        valueofFormula= getCorelationrelationships(df_rule,k, nr_combinations, dep_vars_pairwise_combinations)
        corrMatrix= valueofFormula['corrMatrix']
        if len(valueofFormula['value']) != 0:
            RulesDictf['rule'] ='Formula'
            RulesDictf['operator']  = 'Shouldbe'
            RulesDictf['type']='SIMPLE'
            RulesDictf['value'] =  valueofFormula['value']
            RulesDictf['format']  = ''
            RulesDictf['dimension'] =  'Accuracy'
    return RulesDictf

from sqlalchemy import create_engine, false, true
import sqlite3
@app.route('/connecttosqldb',methods=['GET'])
def connecttosqldb():
    
    csv_database = create_engine('sqlite:///flights.db')
    df = pd.read_sql_query('SELECT * FROM tblflights limit 5000', csv_database)
    data_dict = df.to_dict('index')

    content={}
    content['Preview'] = data_dict
    jsonString = json.dumps(content, default=str)
    return jsonString


def getdffromsql(dbName):
    
    csv_database = create_engine(dbName)
    df = pd.read_sql_query('SELECT * FROM tblSource limit 3000000', csv_database)  
    df.drop('index', axis=1, inplace=True)  
    return df


def getdistinct(keyname):
    
    csv_database = create_engine('sqlite:///flights.db')
    df = pd.read_sql_query('SELECT distinct ('+keyname+' ) as DVALUES  FROM tblflights limit 500', csv_database)
    
    data_dict = df['DVALUES'].values.tolist()
    
    groupList=[]
    for eachvalue in data_dict:
        dictList={}
        df = pd.read_sql_query('SELECT *  FROM tblflights  where '+keyname+'="'+eachvalue+'"  limit 5000', csv_database)
        dictList["keyName"]= eachvalue
        dictList["frame"]=df
        groupList.append(dictList)
    return groupList

@app.route('/querytosqldb',methods=['GET'])
def querytosqldb():
    csv_database = create_engine('sqlite:///flights.db')
    content = request.get_json()
    sourcepath = content["sourcepath"]
    
    tablename=content["tablename"]
    chunksize=2000
    
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext==".csv"):
        chunksize = 200000
        i = 0
        j = 1
        with pd.read_csv(sourcepath, chunksize=chunksize) as reader:
            for chunk in reader:
                df= chunk
                df.index += j
                i+=1
                df.to_sql(tablename, csv_database, if_exists='append')
                j = df.index[-1] + 1
    
    df = pd.read_sql_query('SELECT * FROM ['+ tablename + ']  limit 1000', csv_database)
    data_dict = df.to_dict('index')

    content={}
    content['Preview'] = data_dict
    jsonString = json.dumps(content, default=str)
    return jsonString

@app.route('/checkConnectivitySQL',methods=['GET'])
def checkConnectivitySQL():
    SaveFileAsSQlLite("flights - Copy.csv" ,"sqlite:///S1Flights200k.db")
    df= getdffromsql("sqlite:///S1Flights200k.db")
    data_dict = df.to_dict('index')

    content={}
    content['Preview'] = data_dict
    jsonString = json.dumps(content, default=str)
    return jsonString

def SaveFileAsSQlLite(sourcepath,sqlName):
   
    #content = request.get_json()
    #sourcepath = content["sourcepath"]
    #sqlName = content["sqlName"]
    csv_database = create_engine(sqlName)
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    tablename= "tblSource"
    if(file_ext==".csv"):
        chunksize = 200000
        i = 0
        j = 1
        with pd.read_csv(sourcepath, chunksize=chunksize) as reader:
            for chunk in reader:
                df= chunk
                df.index += j
                i+=1
                df.to_sql(tablename, csv_database, if_exists='append')
                j = df.index[-1] + 1    
    
    return sqlName

# old
@app.route('/testcsvchunks',methods=['GET'])
def testcsvchunks():
    '''Returns a pandas dataframe when given a source path'''
    content = request.get_json()
    sourcepath = content["sourcepath"]
    tablename=content["tablename"]
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext==".csv"):
        csv_database = create_engine('sqlite:///csv_database.db')
        chunksize = 10 
        i = 0
        j = 1
        with pd.read_csv(sourcepath, chunksize=chunksize) as reader:
            for chunk in reader:
                df= chunk
                df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
                df.index += j
                i+=1
                df.to_sql(tablename, csv_database, if_exists='append')
                j = df.index[-1] + 1
        # n=10
        # if(int(len(df)>800)):
        #     #size= (int(len(df)*(n/100)))
        #     size=1000
        #     df = pd.read_sql_query('SELECT * FROM table1 where AIRLINE="AA"', csv_database,chunksize=size)
        # else:
        #     df = pd.read_sql_query('SELECT * FROM table1 where AIRLINE="AA" and ORIGIN_AIRPORT="AUS"', csv_database)

    elif(file_ext==".xls"):
        df = pd.read_excel(sourcepath)
        csv_data = df.to_csv(index = False)
        df = pd.read_csv(StringIO(csv_data))
    elif(file_ext==".xlsx"):
        df = pd.read_excel(sourcepath)
        csv_data = df.to_csv(index = False)
        df = pd.read_csv(StringIO(csv_data))
    elif(file_ext==".json"):
            data = json.load(open(sourcepath,"r"))
            print("Type of JSON data:",type(data))
            if type(data) == dict and len(list(data.keys())) != 0:
                msg1 = "JSON file contains the root key(s):- {}.Please reformat JSON file without any root keys.".format(list(data.keys()))
                msg2 = "JSON file should store the tabular data in a list of dictionaries like this [{},{},{}..] WITHOUT any root keys."
                err_msg = {"Error":msg1+msg2}
                return err_msg
            else:
                df =  pd.DataFrame(data)
                csv_data = df.to_csv(index = False)
                df = pd.read_csv(StringIO(csv_data))
    df = force_datetime_conversion(df) ##Force DataType conversion to suitable date/time cols

    return df_to_json(df.reset_index(drop=True))
#old end
@app.route("/api/checkformulaResult", methods=["POST"])
def checkformulaResult():

    content= request.get_json()
    r = content['rules']
    sourcepath = content['sourcepath']
    df= df_from_path(sourcepath)
    df ["Age"] = pd.to_numeric(df["Age"], errors='coerce')
    print(df.query('((Sex=="male")& (Age<=10))'))
    return 'True'

@app.route('/api/checkupload', methods=['POST'])
def checkupload():
    uploaded_files  = request.files.getlist("file[]")
    fileize=0
    sourceFilename="testfile.csv"
    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext in app.config['UPLOAD_EXTENSIONS']:
                    dli=[]
                    file.save(sourceFilename)
                    fileize= os.path.getsize(sourceFilename)
    return str(fileize/(1024*1024))

@app.route('/api/getPreview', methods=['POST'])
def getPreview():
    uploaded_files = request.files.getlist("file[]")
    sourceFilename="testfile"
    content={}
    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                print(file_ext)
                sourceFilename=sourceFilename+file_ext
                file.save(sourceFilename)
                fileize= os.path.getsize(sourceFilename)
                filesize = fileize/(1024*1024)
                print(filesize)
                file=sourceFilename
                dli=[]
                try:
                    if(file_ext==".csv"):
                        try:
                            print("in csv")
                            str_dem = find_delimiter(file)
                            print("delimiter",str_dem)
                            if(str_dem =="True"):
                                print("in if condition")
                                if filesize>10:                    
                                    chunksize = 50
                                    df= getchunkforPreview(file,chunksize)
                                else:
                                    df= df_from_path(file)
                            else:
                                raise pd.errors.ParserError
                        except(pd.errors.ParserError):
                            data_dict={}
                            content['errorMsg']= 'Upload File Not A Valid CSV File'
                            content['errorflag']= 'True'
                            content['errorCode']= '105'
                            return json.dumps(content, default=str)
                    else : 
                        if filesize>10:                    
                            chunksize = 50
                            df= getchunkforPreview(file,chunksize)
                        else:
                            df= df_from_path(file)
                    print(df.columns.values.tolist())
                    dli= list(df.columns.values)
                    if any ('Unnamed' in col for col in df.columns.values.tolist()):
                                print('Missing Header Columns')
                                raise pd.errors.ParserError
                    if (any(len(ele.strip()) == 0 for ele in df.columns.values.tolist())):
                                print(df.columns.values.tolist())
                                print('Missing Header Columns with empty')
                                raise pd.errors.ParserError
                    df.index = df.index + 2
                    #df=df.head()
                    resDf = df.where(pd.notnull(df), 'None')
                    data_dict = resDf.to_dict('index')    
                except (pd.errors.ParserError, pd.errors.EmptyDataError):
                        data_dict={}
                        content['errorMsg']= 'Data Parsing Failed. Missing headers AND/OR empty columns'
                        content['errorflag']= 'True'
                        content['errorCode']= '105'
                        return json.dumps(content, default=str)
    
    content['sourcePreview']=data_dict
    content['sourceColumns']=dli
    jsonString = json.dumps(content, default=str)
    return jsonString


def checkMaxFileSize(filepath, tolerancelimit):
    fileize= os.path.getsize(filepath)
    filesize = fileize/(1024*1024)
    if filesize > tolerancelimit :
        return True
    else:
        return False


@app.route('/api/getPreviewTest', methods=['POST'])
def getPreviedsdw():
    uploaded_files = request.files.getlist("file[]")
    sourceFilename="testfile"
    content={}
    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                print(file_ext)
                sourceFilename=sourceFilename+file_ext
                file.save(sourceFilename)
                fileize= os.path.getsize(sourceFilename)
                filesize = fileize/(1024*1024)
                print(filesize)
                file=sourceFilename
                dli=[]
                if 1==1:
                    if filesize>10:                    
                        chunksize = 50
                        df= getchunkforPreview(file,chunksize)
                    else:
                        df= df_from_path(file)
                    print(df.columns.values.tolist())
                    dli= list(df.columns.values)
                    if any ('Unnamed' in col for col in df.columns.values.tolist()):
                                print('Missing Header Columns')
                                raise pd.errors.ParserError
                    if (any(len(ele.strip()) == 0 for ele in df.columns.values.tolist())):
                                print(df.columns.values.tolist())
                                print('Missing Header Columns with empty')
                                raise pd.errors.ParserError
                    df.index = df.index + 2
                    #df=df.head()
                    resDf = df.where(pd.notnull(df), 'None')
                    data_dict = resDf.to_dict('index')    
                
    
    content['sourcePreview']=data_dict
    content['sourceColumns']=dli
    jsonString = json.dumps(content, default=str)
    return jsonString



def convert_unit(size_in_bytes, unit):
    return size_in_bytes/(1024*1024)
#    if unit == SIZE_UNIT.KB:
#        return size_in_bytes/1024
#    elif unit == SIZE_UNIT.MB:
#        return size_in_bytes/(1024*1024)
#    elif unit == SIZE_UNIT.GB:
#        return size_in_bytes/(1024*1024*1024)
#    else:
#        return size_in_bytes

def getchunkforPreview(sourcepath,chunksize):
    '''save as a sqllite db when uploading a large file'''
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    i = 0
    j = 1
    if(file_ext==".csv"):
        with pd.read_csv(sourcepath, chunksize=chunksize) as reader:
            for chunk in reader:
                df= chunk
                break                       
          
    elif(file_ext==".xls"):
            df_chunk = pd.read_excel(sourcepath, nrows=chunksize)
            df= df_chunk
            csv_data = df.to_csv(index = False)
            df = pd.read_csv(StringIO(csv_data))
        
    elif(file_ext==".xlsx"):
            df_chunk = pd.read_excel(sourcepath, nrows=chunksize)
            df= df_chunk
            csv_data = df.to_csv(index = False)
            df = pd.read_csv(StringIO(csv_data))
    elif(file_ext==".json"):
            data = json.load(open(sourcepath,"r"))
            print("Type of JSON data:",type(data))
            if type(data) == dict and len(list(data.keys())) != 0:
                msg1 = "JSON file contains the root key(s):- {}.Please reformat JSON file without any root keys.".format(list(data.keys()))
                msg2 = "JSON file should store the tabular data in a list of dictionaries like this [{},{},{}..] WITHOUT any root keys."
                err_msg = {"Error":msg1+msg2}
                return err_msg
            else:
                df =  pd.DataFrame(data)
                csv_data = df.to_csv(index = False)
                df = pd.read_csv(StringIO(csv_data))
    #df = force_datetime_conversion(df) ##Force DataType conversion to suitable date/time cols
    
    return df

def uploadFileAsChunk(sourcepath,dbName):
    '''save as a sqllite db when uploading a large file'''
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    csv_database = create_engine('sqlite:///'+dbName+'.db')
    chunksize = 10 ** 5
    i = 0
    j = 1
    if(file_ext==".csv"):        
        with pd.read_csv(sourcepath, chunksize=chunksize) as reader:
            for chunk in reader:
                df= chunk                
                df.index += j
                df.to_sql('table1', csv_database, if_exists='append')
                j = df.index[-1] + 1          
          
    elif(file_ext==".xls"):
        with pd.read_excel(sourcepath, chunksize=chunksize) as reader:
            for chunk in reader:
                df= chunk
                df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
                df.index += j
                i+=1
                df.to_sql('table1', csv_database, if_exists='append')
                j = df.index[-1] + 1
    elif(file_ext==".xlsx"):
        with pd.read_excel(sourcepath, chunksize=chunksize) as reader:
            for chunk in reader:
                df= chunk
                df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
                df.index += j
                i+=1
                df.to_sql('table1', csv_database, if_exists='append')
                j = df.index[-1] + 1
    
    if os.path.exists(sourcepath):
        os.remove(sourcepath)
        
    
    return 'sqlite:///'+dbName+'.db'

#region globalReference data as sqlite
#this region is replacement of mangoDB with sqllite db
@app.route('/api/Save_MongoDB_GlobalRef_new',methods=['GET'])
def SaveRefsqldb():   
    csv_database = create_engine('sqlite:///csv_database_ref.db')
    content = request.get_json()
    sourcepath = content["sourcePath"]
    dbName=content["dbName"]
    tablename=content["collectionName"]
    chunksize=2000
    
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext==".csv"):
        chunksize = 30000
        i = 0
        j = 1
        with pd.read_csv(sourcepath, chunksize=chunksize) as reader:
            for chunk in reader:
                df= chunk
                df.index += j
                i+=1
                df.to_sql(tablename, csv_database, if_exists='append')
                j = df.index[-1] + 1
    
    df = pd.read_sql_query('SELECT * FROM ['+ tablename+']', csv_database)
    dli= list(df.columns.values)
    AddDBlocalRef("Global_RefData", dbName,tablename,dli)
    #data_dict = df.to_dict('index')

    #content={}
    #content['Preview'] = data_dict
    #jsonString = json.dumps(content, default=str)
    return 'true'

def createRefsqltable(sourcepath,dbName,tablename):   
    csv_database = create_engine('sqlite:///csv_database_ref.db')
    
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext==".csv"):
        chunksize = 30000
        i = 0
        j = 1
        with pd.read_csv(sourcepath, chunksize=chunksize) as reader:
            for chunk in reader:
                df= chunk
                df.index += j
                i+=1
                #df = df.rename(columns=lambda x: x.replace(' ', ''))
                df.to_sql(tablename, csv_database, if_exists='append')
                j = df.index[-1] + 1
    elif(file_ext==".xls"):
            df = pd.read_excel(sourcepath)
            csv_data = df.to_csv(index=None)
            df = pd.read_csv(StringIO(csv_data))
            #df = df.rename(columns=lambda x: x.replace(' ', ''))
            df.to_sql(tablename, csv_database, if_exists='append')    
    elif(file_ext==".xlsx"):
            i = 0
            j = 1
            df = pd.read_excel(sourcepath)
            csv_data = df.to_csv(index=None)
            df = pd.read_csv(StringIO(csv_data))
            df.to_sql(tablename, csv_database, if_exists='append')

    df = pd.read_sql_query('SELECT * FROM ['+ tablename+']', csv_database)
    dli= list(df.columns.values)
    print(dli)
    AddDBlocalRef("Global_RefData", dbName,tablename,dli)
    #data_dict = df.to_dict('index')

    #content={}
    #content['Preview'] = data_dict
    #jsonString = json.dumps(content, default=str)
    return 'true'


@app.route('/api/MongoCluster_query',methods=['POST'])
def localRefCluster_query():
    '''Returns a pandas dataframe when given a source path'''
    content = request.get_json()
    query = content["query"]
    if "matchcase" in content:
        matchcase="YES"
    else:
        matchcase="NO"
    csv_database = create_engine('sqlite:///csv_database_ref.db')

    with sqlite3.connect('csv_database_ref.db') as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()    
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        ListoftablesResults=[]
        results={}
        listofDictTables=[]
        for tablerow in cursor.fetchall():
            table = tablerow[0]
            cursor.execute("SELECT * FROM [{t}]".format(t = table))
            for row in cursor:
                for field in row.keys():
                    if matchcase== "YES":
                        if(row[field]==query):
                            print(table, field, row[field])
                            listofDictTables.append(table+"!~_~!"+field)
                    else:
                        if(query.lower() in (str(row[field]).lower())):
                            print(table, field, row[field])
                            listofDictTables.append(table+"!~_~!"+field)
        listofDictTables1 = list(set(listofDictTables))
        for eachtable in listofDictTables:
            chunks = eachtable.split('!~_~!')
            tablename=chunks[0]
            fieldName=chunks[1]
            if matchcase== "YES":
                df = pd.read_sql_query('SELECT * FROM ['+tablename + '] where ['+fieldName+ '] ="'+query+'"', csv_database) 
            else:
                df = pd.read_sql_query('SELECT * FROM ['+tablename + '] where lower(['+fieldName+ ']) LIKE "%'+query+'%"', csv_database) 
            df.drop('index', axis=1, inplace=True)            
            results.update({tablename:df.to_dict('records')})
    if len(results) == 0:
        results = {"msg":"No results from Query"}
    return results

def AddDBlocalRef(typeName, dbname,collectionName,availablecolumns):
    tabledetails={}
    tabledetails["type"]=typeName
    tabledetails["db"]=dbname
    tabledetails["collection"]=collectionName
    tabledetails["availablecolumns"]= availablecolumns
    x = datetime.now()
    tabledetails["createdAt"]=str(x)

    with open('DBlocalRefTables.json', 'r') as openfile:
            json_object = json.load(openfile)
    data=json_object
    unalterNotificationList=data['tables']
    unalterNotificationList.append(tabledetails)
    
    data['tables']=unalterNotificationList
    json.dump(data, open("DBlocalRefTables.json","w"))

    return 'true'

@app.route('/api/MongoDB_Collection_Preview', methods=['POST'])
def LocalDB_Collection_Preview():
    '''Preview an entrire collection in a database'''
    content = request.get_json()
    db = content["db"]
    tablename = content["collection"]
    start_index = content["start_index"]
    end_index = content["end_index"]
    csv_database = create_engine('sqlite:///csv_database_ref.db')
    df = pd.read_sql_query('SELECT * FROM ['+tablename +']  limit '+ str(end_index), csv_database)     
    df.drop('index', axis=1, inplace=True) 
    df = df.where(pd.notnull(df), 'None')
    return {"Preview":df.to_dict('records')}



def get_df_RefSQLlite(db,tablename):
    '''get df from sqllite a database'''
    csv_database = create_engine('sqlite:///csv_database_ref.db')
    df = pd.read_sql_query('SELECT * FROM ['+tablename +']', csv_database)     
    
    return df


@app.route('/api/GetDB_Collections', methods=['POST'])
def GetrefDB_Collections():
    with open('DBlocalRefTables.json', 'r') as openfile:
            json_object = json.load(openfile)
    data=json_object
    unalterNotificationList=data['tables']
    dblist=(([d["db"] for d in unalterNotificationList ]))                
    dblist = list(set(dblist))
    clusterDict={}
    for eachtable in dblist:
        clusterlist=(([d for d in unalterNotificationList if d["db"]== eachtable]))          
        clusterlist=(([d["collection"] for d in clusterlist]))
        clusterlist = list(set(clusterlist))
        clusterDict[eachtable]=clusterlist
    clustercontentDict={}
    for eachtable in unalterNotificationList:
       clustercontentDict[eachtable["collection"]] = eachtable["availablecolumns"]
    return {"Cluster_Contents":clusterDict, "Databases":dblist,  "Collection_Contents":clustercontentDict}


@app.route('/api/GetDB_category', methods=['POST'])
def GetrefDB_category():
    with open('DBlocalRefTables.json', 'r') as openfile:
            json_object = json.load(openfile)
    data=json_object
    unalterNotificationList=data['tables']
    dblist=(([d["db"] for d in unalterNotificationList ]))                
    dblist = list(set(dblist))
   
    return {"Databases":dblist}



def GetrefDB_collections(db):
    with open('DBlocalRefTables.json', 'r') as openfile:
            json_object = json.load(openfile)
    data=json_object
    unalterNotificationList=data['tables']  
    clusterlist=(([d for d in unalterNotificationList if d["db"]== db]))          
    clusterlist=(([d["collection"] for d in clusterlist]))
    clusterlist = list(set(clusterlist))
    return clusterlist

def GetrefDB_columns(db):
    with open('DBlocalRefTables.json', 'r') as openfile:
            json_object = json.load(openfile)
    data=json_object
    unalterNotificationList=data['tables']  
    clusterlist=(([d for d in unalterNotificationList if d["db"]== db]))          
    clusterlist=(([d["availablecolumns"] for d in clusterlist]))
    existing_col_names = list(set(reduce(lambda x,y: x+y, clusterlist)))
    return existing_col_names


@app.route('/api/MongoDB_Collection_Save', methods=['POST'])
def localRefDB_Collection_Save():
    '''Preview an entrire collection in a database'''
    content = request.get_json()
    db = content["db"]
    col = content["collection"]
    output_filename = content["output_filename"]
    csv_database = create_engine('sqlite:///csv_database_ref.db')
    df = pd.read_sql_query('SELECT * FROM ['+col +']', csv_database)     
    df.drop('index', axis=1, inplace=True)
    output_path = "mongodb_data/{}".format(output_filename)
    df.to_csv(output_path, index = False)
    return {"Message": "MongoDB Collection Saved", "outputpath" : output_path}


@app.route('/api/profile_GlobalRef', methods=['POST'])
def profile_GlobalRef():
    '''Preview an entrire collection in a database'''
    content = request.get_json()
    db = content["db"]
    col = content["collection"]
    output_filename = content["output_filename"]
    csv_database = create_engine('sqlite:///csv_database_ref.db')
    df = pd.read_sql_query('SELECT * FROM  ['+col +']' , csv_database)     
    df.drop('index', axis=1, inplace=True)
    output_path = "mongodb_data/{}".format(output_filename)
    jsonString= profileAnalysisRef(output_path,df)
    return jsonString

@app.route('/api/Save_MongoDB_GlobalRef', methods=['GET'])
def Save_localDB_GlobalRef():
    db_c_saved_paths=[]
    return {"Ref_data_files":db_c_saved_paths}

@app.route('/api/createNewMongoDBSource', methods=['POST'])
def createNewlocalRefDBSource():
    """
    Create New Source for Profile and Data Cleaning Modules
    """
    uploaded_files = request.files.getlist("file[]")
    content = json.loads(request.form.get('data'))
    db = content["db"]
    col = content["collection"]
    

    ## Check if db and collection are already existing
    existing_collection_names= GetrefDB_collections(db)
    
    if col in existing_collection_names:
        return {'errorMsg' : "Collection Name already exists. Please input alternative collection name!", "errorCode" : '110', 'errorflag': 'True' }
    ##check if the column name is already existing
    existing_column_names= GetrefDB_columns(db)

    for file in uploaded_files:
        filename = secure_filename(file.filename)
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                file.save(filename)
                try:
                    df = df_from_path(filename)
                    createRefsqltable(filename,db,col)
                    if any ('Unnamed' in col1 for col1 in df.columns.values.tolist()):
                            error_msg = "Missing headers AND/OR empty columns"
                            print(error_msg)
                            raise pd.errors.ParserError ## ['.','$','*',':','|','?']
                    compare_cols = [col for col in df.columns.values.tolist() if col in existing_column_names]
                    if len(compare_cols)>0:
                            return {'errorMsg' : "At least one column name is matching with the available columns in the database", "errorCode" : '110', 'errorflag': 'True' }
                except (pd.errors.ParserError, pd.errors.EmptyDataError):
                    content = {}
                    content['errorMsg']= 'Data Parsing Failed. {}'.format(error_msg)
                    content['errorflag']= 'True'
                    content['errorCode']= '105'
                    return json.dumps(content, default=str)
    
    os.remove(filename)
    return {"message" : "SUCCESS -  documents uploaded to  Collection"}
#endregion
@app.route('/api/GetAllDataCatalogues', methods=['GET'])
def GetAllDataCatalogues():    
    jsonString=getcatalogue()
    return jsonString

@app.route('/api/createDataCatalogue', methods=['POST'])
def createDataCatalogue():
    content = request.get_json()   
   
    jsonString=createDatacatalogue(content)
    return jsonString

@app.route('/api/createDataCatalogue', methods=['PUT'])
def editDataCatalogue1():
    content = request.get_json()   
   
    jsonString=editDatacatalogue(content)
    return jsonString    
        
@app.route('/api/stdzn', methods=['POST']) 
def stdzn():
    import pytz
    content = request.get_json()
    sourcepath= content['sourcepath']
    df = df_from_path(sourcepath)
# Load the JSON file
    with open("data.json", "r") as file:
        json_obj = json.load(file)
# Load the JSON file
    with open("data2.json", "r") as file:
        json_obj1 = json.load(file)
    col_names= list(df.columns)
    for i in col_names:    
        col_list =  list(df[i])
#         print(col_list)
        new_list=[]
        for j in col_list:
            txt=str(j)
            regular_expression = r'[^a-zA-Z]'
            new_string = re.sub(regular_expression, '', txt).upper()
            new_list.append(new_string)
            df_new = pd.DataFrame(new_list,columns=[i])
            if i == 'country':
                df[i]=df_new[i].map(pytz.country_names)
            elif i == 'us_states':
                df[i]=df_new[i].map(json_obj[i])
            elif i == 'in_states':
                df[i] = df_new[i].map(json_obj1[i])                
            else:
                df[i]=df[i]
    json_str = df.to_json(orient='records') 
    return json_str   

    
@app.route('/api/deanonymize', methods=['POST']) 
def deanonymize():
    from sklearn.preprocessing import LabelEncoder
    import pickle
    # content = request.get_json()
    # sourcepath= content['sourcepath']
    # encoded_df = df_from_path(sourcepath)
    encoded_df=pd.read_csv('encoded_data.csv')
    with open('selected_cols.txt', 'r') as file:
        selected_cols = file.read().splitlines()
    with open('encoders.pkl', 'rb') as file:
        encoders = pickle.load(file)
    with open('scaler.pkl', 'rb') as file1:
        scaler = pickle.load(file1)
    for col in encoders:
        encoded_df[col] = encoders[col].inverse_transform(encoded_df[col])
        encoded_df = encoded_df.reset_index(drop=True)
    numerical_cols = [col for col in selected_cols if encoded_df[col].dtype in ["float64", "int64"]]
    encoded_df[numerical_cols] = scaler.inverse_transform(encoded_df[numerical_cols])
    encoded_df = encoded_df.reset_index(drop=True)
    encoded_df.to_csv('decoded_data.csv', index=False)
    json_string = encoded_df.to_json(orient='records')
    return json_string 

@app.route('/api/sqlite_Collection_download', methods=['POST'])	
def LocalDB_Collection_download():	
    '''Preview an entrire collection in a database'''	
    content = request.get_json()	
    db = content["db"]	
    tablename = content["collection"]	
    start_index = content["start_index"]	
    end_index = content["end_index"]	
    csv_database = create_engine('sqlite:///csv_database_ref.db')	
    df = pd.read_sql_query('SELECT * FROM ['+tablename +']  limit '+ str(end_index), csv_database)     	
    df.drop('index', axis=1, inplace=True) 	
    df = df.where(pd.notnull(df), 'None')	
    output_path=tablename+db+".csv"	
    df.to_csv(output_path, index = False)	
    return {"Preview":"true"}      

def GetAEntireLaunchEntityDB_profile(sourceId):
    analysisList={}
    entiresourceList=[]
    entiresourceObj={}
    entiresourceObj['uploads']=[]
    data={}
    with open('uploadProfiledb.json', 'r') as openfile:
                    json_object = json.load(openfile)

    data = json_object
    result={}
    for obj in data['uploadList']:
        if obj["sourceId"]==sourceId:
            entiresourceObj=obj
        else:
            entiresourceList.append(obj)
    result['EntireSourceObj'] = entiresourceObj
    result['EntireSourceList'] = entiresourceList
    jsonString = json.dumps(result, default=str)
    return jsonString


def removeALaunchEntityDB_profile(sourceId):
    analysisList=[]
    data={}

    with open('uploadProfiledb.json', 'r') as openfile:
                    json_object = json.load(openfile)

    data = json_object
    result={}
    for obj in data['uploadList']:
        if obj["sourceId"]!=sourceId:
             analysisList.append(obj)
    result={}
    result['uploadLists'] = analysisList
    jsonString = json.dumps(result, default=str)
    return jsonString


@app.route('/api/uploadSource_profile', methods=['POST'])
def uploadSource_profile():
    uploaded_files = request.files.getlist("file[]")
    print(uploaded_files)
    content =json.loads(request.form.get('data'))
    sourceId= content['sourceId']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['SourceSettings']
    settings= {}

    if ("frequency" in sourcedetails ):
       settings["frequency"] = sourcedetails["frequency"]
       settings["expectedUploadDate"]= sourcedetails["expectedUploadDate"]
       settings["expecteduploadTime"]= sourcedetails["expecteduploadTime"]
    else:
        settings["frequency"]="Daily" 
        settings["expectedUploadDate"]=""
        settings["expecteduploadTime"]=""
    

    if 'isOriginalSource' in content:
        isOriginalSource= content['isOriginalSource'] # YES /NO
        OriginalSourcePath= content['OriginalSourcePath']        
    else:
        isOriginalSource= 'NO'
        OriginalSourcePath= ''

    
    sourceFilename = sourcedetails['sourceFileName']
    sourcePath=''
    #keyNametoLaunch= sourceCatColumns[0]
    newuploadId=sourceId+'U'+ str(int(getUploadMaxId_profile(sourceId))+1)
    sourcename = os.path.splitext(sourceFilename)[0]
    print(sourcename)
    sourceFilename=newuploadId + sourcename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB_profile(sourceId))
    print('launch entity in upload source oracle')
    
    EntireSourceObj= LaunchEntityRaw['EntireSourceObj']
    EntireSourceList= LaunchEntityRaw['EntireSourceList']
    uploadsObject= EntireSourceObj['uploads']
 
    typename=""
    connectionDetails={}
    if "type" in sourcedetails:
        if sourcedetails["type"]== "oracle":
            if sourcedetails["type"]== "oracle":
                typename= "oracle"
                connectionDetails=sourcedetails["connectionDetails"]
                connContent=sourcedetails["connectionDetails"]
                host=connContent['host']
                dbName = connContent['dbName']
                userName = connContent['username']
                port = connContent['port']
                password = connContent['password']
                table = connContent['sourceTableName']
                #df = getdfFromTable(host,dbName,port,userName,password,table)
                dli= list(df.columns.values)

                sourceavailableColumns=sourcedetails['availableColumns']
                if dli != sourceavailableColumns:
                    content['errorMsg']= 'The file headers does not match with the configured source'
                    content['errorflag']= 'True'
                    content['errorCode']= '103'
                    return json.dumps(content)
        
    elif isOriginalSource=="YES":
        df= df_from_path(OriginalSourcePath)
        dli= list(df.columns.values)
        #sourceavailableColumns=sourcedetails['availableColumns']
        sourcePath=OriginalSourcePath
        # if dli != sourceavailableColumns:
        #         content['errorMsg']= 'The file headers does not match with the configured source'
        #         content['errorflag']= 'True'
        #         content['errorCode']= '103'
        #         return json.dumps(content)
    else:
        for file in uploaded_files:
            filename = secure_filename(file.filename)
            if filename != '':
                    print(sourceFilename)
                    print(filename)
                    file_ext = os.path.splitext(filename)[1]
                    file_identifier = os.path.splitext(filename)[0]
                    sqllitepath= 'sqlite:///'+file_identifier+'.db'
                    # sqllitepath= 'sqlite:///'+sourceFilename+'.db'
                    # sourceFilename=sourceFilename+file_ext
                    # print(file_ext)
                    if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                        dli=[]
                        # sourcePath=sourceFilename
                        sourcePath = newuploadId+ filename
                        file.save(sourcePath)
                        
                        try:
                            fileize= os.path.getsize(sourcePath)
                            filesize = fileize/(1024*1024)
                            if filesize>15:
                                 i=0
                                #SaveFileAsSQlLite(sourcePath,sqllitepath)                            
                        except:
                           i=1

                        
                        df= df_from_path(sourcePath)
                        dli= list(df.columns.values)

                        sourceavailableColumns=sourcedetails['availableColumns']


                        if dli != sourceavailableColumns:
                            content['errorMsg']= 'The file headers does not match with the configured source'
                            content['errorflag']= 'True'
                            content['errorCode']= '103'
                            return json.dumps(content)
                        

    print(uploadReason)
    if(uploadReason==''):
        
        if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                            payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
    else:
        if uploadReason!="cleansedsource":
                if(settings['frequency']=="Daily"):
                    print("in one")
                    editedUploadObj=[]
                    for item in uploadsObject:
                                
                                payloadDate =  datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                                actualDate = datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
                                if payloadDate.date()!=actualDate.date():
                                                                 
                                    editedUploadObj.append(item)
                    uploadsObject= editedUploadObj
                    print("in four")    
                    print(uploadsObject)         

    # df= df_from_path(mpath)                       


    resultContent={}

    resultContent['ProfileId']=sourceId
    resultContent['uploadId']=newuploadId
    #resultContent['rulesetId']=rulesetId
    resultContent['sourceFileName']=sourcePath
    resultContent['sourcePath']=sourcePath
    resultContent['type']=typename
    resultContent['connectionDetails']=connectionDetails
    resultContent['uploadDate']=uploadDate
    resultContent['uploadTime']=uploadTime
    resultContent['isexpecteddate']='Yes'
    resultContent['reference']=[]
    resultContent['uploadList']={}
    #resultContent['isCleanedSource']=isCleanedSource
    resultContent['isOriginalSource']=isOriginalSource
    resultContent['SourceSettings']= sourcedetails
    #resultContent['settings']=settings
    resultList=[]

    #resultContent['AnalysisResultList']=''
    content['uploadId']=newuploadId
    content['sourceFilename']=sourceFilename
    uploadsObject.append(resultContent)
    EntireSourceObj['uploads']=uploadsObject
    EntireSourceObj['sourceId']=sourceId
    EntireSourceList.append(EntireSourceObj)
    data={}
    data['uploadList']=EntireSourceList
    jsonString= json.dumps(data, default=str)
    json.dump(data, open("uploadProfiledb.json","w"), default=str)
    content1={}
    content1['errorMsg']= ''
    content1['errorflag']= 'False'
    content1['responseObj']= jsonString

    tempDict={}
    tempDict['sourceId']= sourceId
    tempDict['uploadId'] =newuploadId
    tempDict['source']= sourcedetails
    uploadSourceDetail= {}
    uploadSourceDetail['uploadId'] =newuploadId
    uploadSourceDetail['uploadDate'] =uploadDate
    uploadSourceDetail['uploadTime'] =uploadTime
    uploadSourceDetail['sourceFileName']=sourceFilename
    tempDict['rules']=[]
    tempDict["UploadsHistory"]= []
    tempDict['recentsourceUpload'] = uploadSourceDetail
    tempDict['uploadFilePath']=sourcePath

    inputcontent1={}
    inputcontent1['sourceId']=sourceId
    inputcontent1['uploadId']=newuploadId
    #inputcontent1['rulesetId']=rulesetId
    #inputcontent1['keyname']=sourceCatColumns


    
    tempDict['stuats'] ='started'
    

    return json.dumps(tempDict, default=str)




def getUploadMaxId_profile(sourceId):
        with open('uploadProfiledb.json', 'r') as openfile:
                    json_object = json.load(openfile)
        data = json_object
        IDList=[]
        IDList.append('0')
        for obj in data['uploadList']:
                if obj["sourceId"]==sourceId:
                   for obj1 in obj["uploads"]:
                        IDList.append(obj1["uploadId"][(len(sourceId)+1):] )
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))

@app.route('/api/reference_data', methods=['POST']) 
def reference_data():
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
    if sourcePath!="":                    
            
            source_df = df_from_path_uploadSource(sourcepath)
            source_columns = source_df.columns.tolist() 
            ref1_file = 'worldCitiesGeoData.csv'
            ref2_file = 'Gender11.csv'
            ref3_file = 'seaportCodesTravel.csv'
            ref4_file = 'airportCodesTravel.csv'
            ref5_file = 'nse_stocksFinance.csv'
            ref6_file = 'nasdaq_stocksFinance.csv'
            ref7_file = 'nyse_stocksFinance.csv'
            ref8_file = 'global_exchangesFinance.csv'
        # store the reference files in a list or dictionary
            ref_files = [ref1_file, ref2_file, ref3_file, ref4_file, ref5_file,ref6_file,ref7_file,ref8_file]
            def convert_to_lower(x):
                if type(x) == str and any(c.isalpha() for c in x):
                    return x.lower()
                else:
                    return x
            source_df = source_df.applymap(convert_to_lower)
        # initialize a list to store the file names of reference files with common data
            common_files = []
            common_data_list = []
        # loop through the reference files and check if there is any common data with the source dataframe
            for ref_file in ref_files:
                ref_df = pd.read_csv(ref_file)
                ref_df = ref_df.applymap(convert_to_lower)
                common_data = set(source_df.values.flatten()) & set(ref_df.values.flatten())
                if common_data:
                    # print('Common data found in file: ', os.path.basename(ref_file))
                    for col in ref_df.columns:
                        col_common_data = set(ref_df[col].values.flatten()) & common_data
                        if col_common_data:
                            # convert NumPy int64 objects to Python's int type
                            col_common_data = [int(i) if isinstance(i, np.int64) else i for i in col_common_data]
                            source_file_col_name = [col for col in source_columns if any(val in col_common_data for val in source_df[col])]
                            common_data_dict = {
                                "file_name": os.path.basename(ref_file),
                                "column_name": col,
                                "common_data": col_common_data,
                                "Source_file_col_name":source_file_col_name
                                            }
                            # Calculate confidence level using Jaccard index
                            common_data_set = set(col_common_data)
                            ref_data_set = set(ref_df[col].values)
                            jaccard_index = len(common_data_set.intersection(ref_data_set)) / len(common_data_set.union(ref_data_set))
                            common_data_dict["confidence_level"] = jaccard_index*100
                            common_data_list.append(common_data_dict)
                            common_files.append(os.path.basename(ref_file))
        # print the file names of reference files with common data as a JSON string
            if common_data_list:
                outputdata = json.load(open("DBlocalRefTables1.json","r"))
                resultList=  outputdata['tables']
                responsedata=[]                        
                for eachitem in common_data_list:            
                    sourcepath=eachitem["file_name"]      
                    LDBDataList=[d for d in resultList if (d["Filepath"])==sourcepath]
                    if len(LDBDataList)>0:
                        eachitem["referencedetails"]=LDBDataList[0]
                    # eachitem["source_file_column_name"] = source_col_name
                    responsedata.append(eachitem)
                return json.dumps(responsedata)
            else:
                return ('No common data found in any of the reference files.')
    else:
                return ('No common data found in any of the reference files.')


# def start_flask(**server_kwargs):

#     app = server_kwargs.pop("app", None)
#     server_kwargs.pop("debug", None)

#     try:
#         import waitress

#         waitress.serve(app, **server_kwargs)
#     except:
#         app.run(**server_kwargs)




# if __name__ == "__main__":

#     # app.run(debug=True)
#     mp.freeze_support()
#     # Default start flask
#     FlaskUI(
#         app=app,
#         server="flask",
#         width=800,
#         height=600,
#     ).run()

#     # Default start flask with custom kwargs

#     # FlaskUI(
#     #     server="flask",
#     #     server_kwargs={
#     #         "app": app,
#     #         "port": 3002,
#     #     },
#     #     width=800,
#     #     height=600,
#     # ).run()

#     # Custom start flask

#     # def saybye():
#     #     print("on_exit bye")

#     # FlaskUI(
#     #     server=start_flask,
#     #     server_kwargs={
#     #         "app": app,
#     #         "port": 3000,
#     #         "threaded": True,
#     #     },
#     #     width=800,
#     #     height=600,
#     #     on_shutdown=saybye,
#     # ).run()

if __name__ == '__main__':
    app.run()