

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
from itertools import combinations,chain
import numpy as np
from ClassifyColumns import classify_columns
from sendMail import sendMailtoUser
from common import df_from_path,int_cat_datatype_conv

from dataLineage import EntryDataLineage
import re
from CorrelationFunctions import compute_associations
from pathlib import Path


def key_options(items):
    return chain.from_iterable(combinations(items, r) for r in range(1, len(items)+1) )

def getprimarykey(sourcePath,vendorName):
    df =df_from_path(sourcePath)
    df.columns = df.columns.str[3:]#str.lstrip(vendorName+"_")
    # iterate over all combos of headings, excluding ID for brevity
    # res=[]
    # for candidate in key_options(list(df)[1:]):
    #     deduped = df.drop_duplicates(candidate)

    #     if len(deduped.index) == len(df.index):
    #         res.append((candidate))
    full_list = chain.from_iterable(combinations(df, i) for i in range(1, len(df.columns)+1))

    n = len(df.index)

    res = []
    i=0
    for cols in full_list:
        
        cols = list(cols)
        if len(df[cols].drop_duplicates().index) == n:
            res.append(cols)
            i=i+1
        if i==10:
            break    
    return   res      

def listSourceNames_mdb():
        with open('dbmdatamodule.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        IDList=[]
        IDList.append('')
        for obj in data['mdbSources']:
            sourceobj= obj['source']
            IDList.append((sourceobj["sourceDataName"]))
        return (IDList)

def removeAEntityDB_mdb(sourceId):
        analysisList=[]
        data={}
        with open('dbmdatamodule.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        for obj in data['mdbSources']:
            if obj["sourceId"]!=sourceId:
                analysisList.append(obj)
        data['mdbSources'] = analysisList
        jsonString = json.dumps(data, default=str)
        return jsonString

def getSourceMaxId_mdb():
        with open('dbmdatamodule.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        dictionary ={}
        IDList=[]
        IDList.append('0')
        for obj in data['mdbSources']:
                    IDList.append((obj["sourceId"])[1:])
        dictionary["IdsList"] = IDList
        print(str(IDList))
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))

def GetAEntityDB_mdb(sourceId):
    analysisList={}
    data={}
    with open('dbmdatamodule.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['mdbSources']:
        if obj["sourceId"]==sourceId:
            analysisList=obj
            break
    data['mdbSources'] = analysisList
    jsonString = json.dumps(data, default=str)
    return jsonString


def removeAEntityDB_mdbglobal(sourceId):
        analysisList=[]
        data={}
        with open('dbmdRules.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        for obj in data['mdbSources']:
            if obj["sourceId"]!=sourceId:
                analysisList.append(obj)
        data['mdbSources'] = analysisList
        jsonString = json.dumps(data, default=str)
        return jsonString



def GetAEntityDB_mdbglobal(sourceId):
    analysisList={}
    data={}
    with open('dbmdRules.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['mdbSources']:
        if obj["sourceId"]==sourceId:
            analysisList=obj
            break
    data['mdbSources'] = analysisList
    jsonString = json.dumps(data, default=str)
    return jsonString


def configureSource_mdm1(uploaded_files,content):
    selectedColumns=[]
    sourcedetails= content['source']
    settings= content['settings']
    vendorName=settings['vendorName']
    print(vendorName)
    createdUser= sourcedetails['createdUser']
    data_dict={}
    #source related details
    sourceDataName= sourcedetails['sourceDataName']
    listA = listSourceNames_mdb()
    if sourceDataName in listA:
        content['errorMsg']= 'The source name already exists'
        content['errorflag']= 'True'
        content['errorCode']= '101'
        return json.dumps(content, default=str)
    else:
        #sourceDataDescription= sourcedetails['sourceDataDescription']
        sourceFilename = sourcedetails['sourceFileName']
        newSourceId='S'+ str(int(getSourceMaxId_mdb())+1)
        sourceFilename=newSourceId + sourceFilename
        df = pd.DataFrame()
        path=""
        for file in uploaded_files:
                filename = secure_filename(file.filename)
                if filename != '':
                    file_ext = os.path.splitext(filename)[1]
                    if file_ext in ['.csv', '.xlsx', '.xls', '.json', '.xml']:
                        dli=[]
                        file.save(sourceFilename)
                        try:
                            path=sourceFilename
                            fileize= os.path.getsize(sourceFilename)
                            filesize = fileize/(1024*1024)
                            print(filesize)
                            file=sourceFilename
                            dli=[]
                            print(path)
                            print('am here')
                            df= df_from_path(path)
                            if any ('Unnamed' in col for col in df.columns.values.tolist()):
                                    print('Missing Header Columns')
                                    raise pd.errors.ParserError
                        except (pd.errors.ParserError, pd.errors.EmptyDataError):
                            content['errorMsg']= 'Data Parsing Failed. Missing headers AND/OR empty columns'
                            content['errorflag']= 'True'
                            content['errorCode']= '105'
                            return json.dumps(content, default=str)
            
        if not df.empty:
                    output = classify_columns(df, verbose=0)
                    print(output)
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
                    df_result = df.copy()
                    df=df.head()
                    resDf = df.where(pd.notnull(df), 'None')
                    data_dict = resDf.to_dict('index')
                    dli= list(df.columns.values)
                    sourcedetails['templateSourcePath']= path
                    sourcedetails['templateSourcePath']= path
                    sourcedetails['availableColumns'] = ['PRIMARY ID','ISIN','EXCHANGE',
                    'NAME','INDUSTRY SECTOR',
                    'INDUSTRY GROUP','CURRENT MARKET CAP',
                    'LAST PRICE',
                    'NAME OF THE CEO',
                    'START DATE OF CEO',
                    'CEO COMPENSATION',
                    'COMPANY TELEPHONE NUMER',
                    'COMPANY WEB ADDRESS']
                    # sourcedetails['availableColumns'] = ['RE_PRIMARY ID','RE_ISIN','RE_EXCHANGE',
                    # 'RE_NAME','RE_INDUSTRY SECTOR',
                    # 'RE_INDUSTRY GROUP','RE_CURRENT MARKET CAP',
                    # 'RE_LAST PRICE',
                    # 'RE_NAME OF THE CEO',
                    # 'RE_START DATE OF CEO',
                    # 'RE_CEO COMPENSATION',
                    # 'RE_COMPANY TELEPHONE NUMER',
                    # 'RE_COMPANY WEB ADDRESS']
                    sourcedetails['sqlLitePath']='sqlite:///'+newSourceId+sourceDataName+'.db'
                    try:
                        fileize= os.path.getsize(path)
                        filesize = fileize/(1024*1024)
                        if filesize>15:
                            #SaveFileAsSQlLite(path,'sqlite:///'+newSourceId+sourceDataName+'.db')
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
                    content['source']=  sourcedetails
                    content['sourcePreview']=data_dict
                    sourcelogresult=savesource(df_result,sourceFilename,vendorName)
                    content['sourcelogs']=sourcelogresult
                    content['matchingKey']=["ISIN"]
                    if sourcelogresult['level2Stdfilepath']!="":
                        content['matchingKeynew']=getprimarykey(sourcelogresult['level2Stdfilepath'],vendorName)
                    else:
                        content['matchingKey']=["ISIN"]    
    data = json.load(open("dbmdatamodule.json","r"))
    AnalysisList=  data['mdbSources']
    content['sourceId']= newSourceId
    content['rules']=[]
    AnalysisList.append(content)
    data['mdbSources'] = AnalysisList
    json.dump(data, open("dbmdatamodule.json","w"), default=str)
    newsourcedetails= sourcedetails
    newsourcedetails['sourceId']= newSourceId
    newsourcedetails['rules']=[]
    newsourcedetails["sourcePath"]= sourcedetails['templateSourcePath']
    newsourcedetails["department"]=settings["department"]
    CreateRulesetmdm_globaldefault( settings['datatypeVendor'])
    #updatecommondb("dqm",newsourcedetails)
    content['errorMsg']= ''
    content['errorflag']= 'False'
    messagel = "File Received in MDM"
    EntryDataLineage(sourcedetails['templateSourcePath'],newSourceId,  sourceDataName,newSourceId, str(datetime.datetime.now()),"MDM",messagel,selectedColumns)
     
    return json.dumps(content, default=str)


def checkSourceNameAvailability_Ndm(content):
   
    sourceDataName = content["sourceName"]
    data_dict={}
    listA = listSourceNames_mdb()
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

def editconfiguredsource(uploaded_files,content):
    sourcedetails= content['source']
    sourceId=content['sourceId']
    referencedetails= content['reference']
    settings= content['settings']
    data_dict={}
    datafromdb=  json.loads(removeAEntityDB_mdb(sourceId))
    allDBList= datafromdb['mdbSources']
    tobeedited= json.loads(GetAEntityDB_mdb(sourceId))
    datatobeedited= tobeedited['mdbSources']
    rules=datatobeedited['rules']
    #source related details
    sourceDataName= sourcedetails['sourceDataName']
    listA= listSourceNames_mdb()
    #sourceDataDescription= sourcedetails['sourceDataDescription']
    sourceFilename = sourcedetails['sourceFileName']
    newSourceId=sourceId
    datasource= datatobeedited['source']
    isqllitesource= "No"
    print(datasource)
    sourceFilename= newSourceId+ ""+ sourceFilename
    j=1
    
    for file in uploaded_files:
        filename = secure_filename(file.filename)
        print(filename)
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext in ['.csv', '.xlsx', '.xls', '.json', '.xml'] :
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
                            #SaveFileAsSQlLite(path,'sqlite:///'+newSourceId+ sourceDataName+'.db')
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
    content['rules']=rules
    del content["ref_data_type"]
    allDBList.append(content)

    data={}
    data['mdbSources'] = allDBList
    json.dump(data, open("dbmdatamodule.json","w"), default=str)
   
    return json.dumps(content, default=str)

def ismdbSourceValid(userName,userDepartment,sourceDetail):
    source = sourceDetail['source']
    soucedpt=sourceDetail['settings']
    createdUser=source['createdUser']    
    departmentList = soucedpt['department']
    flag = False
    flag=True

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

def getlistfromMDB(content):
    userName=content['userName']
    # userCategory=content['userCategory']
    userDepartment= content['department']
    '''API to retrieve complete cleandb json log file'''
    data = json.load(open("dbmdatamodule.json","r"))
    
    cleandbsingleList=[]
    for listitem in data['mdbSources']:
        if ismdbSourceValid(userName,userDepartment,listitem) :
            cleandbsingleList.append(listitem)
    data={}
    data['SourceDetailsList'] = cleandbsingleList
    
    jsonString = json.dumps(data, default=str)
    return jsonString
 
def getlistfromMDB_vendorBased(content):
    userName=content['userName']
    # userCategory=content['userCategory']
    userDepartment= content['department']
    '''API to retrieve complete cleandb json log file'''
    data = json.load(open("dbmdatamodule.json","r"))
    
    data1 = json.load(open("dbmdRules.json","r"))
    dataresult=[]
    DataList=[d for d in data1['mdbSources'] if (d["sourceId"])!=""]
    for eachvendor in DataList:
        dataresDict={}
        dataresDict["Id"]=eachvendor["sourceId"]
        dataresDict["name"]=eachvendor["sourceId"]
        dataresDict["GlobalRules"]=eachvendor["rules"]
        dataresDict['GoldenSourcePath']=eachvendor["GoldenSourcePath"]
        dataresDict["sourceList"]=[]
        cleandbsingleList=[]
        #data["settings"][""datatypeVendor": "EQUITIES","]
        test_list=data['mdbSources']
        #test_list = [{'vendortype':item['settings']['datatypeVendor'], **item} for item in test_list]
        DataList=[d for d in test_list if (d['settings']['datatypeVendor'])==eachvendor["sourceId"]]
        dataresDict["sourceList"]=DataList
        for d in DataList:
            dataresDict["AvailableColumns"]=d['source']['availableColumns']
            break
        dataresult.append(dataresDict)
    
    jsonString = json.dumps(dataresult, default=str)
    return jsonString


def getlistfromMDB_byvendor(sourcevendor):
    
    data = json.load(open("dbmdatamodule.json","r"))
    list=[]
    if sourcevendor!="":
        
        test_list=data['mdbSources']
        #test_list = [{'vendortype':item['settings']['datatypeVendor'], **item} for item in test_list]
        DataList=[d for d in test_list if (d['settings']['datatypeVendor'])==sourcevendor]
        
        IDList=[]
        
        for obj in DataList:
            sourceobj= obj['sourcelogs']
            IDList.append((sourceobj["level2Stdfilepath"]))
        print(IDList)    
        return (IDList)
    
    
def getlistfromMDB_vendorBasedold(content):
    userName=content['userName']
    # userCategory=content['userCategory']
    userDepartment= content['department']
    '''API to retrieve complete cleandb json log file'''
    data = json.load(open("dbmdatamodule.json","r"))
    
    data1 = json.load(open("dbmdRules.json","r"))
    dataresult=[]
    DataList=[d for d in data1['mdbSources'] if (d["sourceId"])!=""]
    for eachvendor in DataList:
        dataresDict={}
        dataresDict["Id"]=eachvendor["sourceId"]
        dataresDict["name"]=eachvendor["sourceId"]
        dataresDict["GlobalRules"]=eachvendor["rules"]
        dataresDict["sourceList"]=[]
        cleandbsingleList=[]
        #data["settings"][""datatypeVendor": "EQUITIES","]
        test_list=data['mdbSources']
        #test_list = [{'vendortype':item['settings']['datatypeVendor'], **item} for item in test_list]
        DataList=[d for d in test_list if (d['settings']['datatypeVendor'])==eachvendor["sourceId"]]
        dataresDict["sourceList"]=DataList
        for d in DataList:
            dataresDict["AvailableColumns"]=d['source']['availableColumns']
            break
        dataresult.append(dataresDict)
    
    jsonString = json.dumps(dataresult, default=str)
    return jsonString

def getlistfromMDBglobal(content):
    userName=content['userName']
    # userCategory=content['userCategory']
    userDepartment= content['department']
    sourceId= content['sourceId']
    '''API to retrieve complete cleandb json log file'''
    
    data={}
    
    data1 = json.load(open("dbmdRules.json","r"))
    
    DataList=[d for d in data1['mdbSources'] if (d["sourceId"])==sourceId]
    data['GlobalrulesList']=DataList
    jsonString = json.dumps(data, default=str)
    return jsonString




def getrules_mdm(content):

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
            #df = getdfFromTable(host,dbName,port,userName,password,table)
    else:
        df= df_from_path(sourcepath)

    if "settings" in content:
        settings=content["settings"]
        if settings["vendorName"]!= "":
            vendorName=settings["vendorName"]
            df.columns = df.columns.str[3:]#str.lstrip(vendorName+"_")

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

   

    # 06/06/2022- Thiru #51 & #57 (Ruleset formula detection issues)
    # Create new list to hold all the features - Upper / Lower triangle of correlation matrix 
    df_clean = df.select_dtypes(exclude=['object','datetime64[ns]'])
    # df_clean = df_clean.loc[:,df_clean.apply(pd.Series.nunique) != 1]

    results=[]
    for k in selectedColumns:
        r=rulesforSingleColumn_mdm(df,k,conFeat,catFeat,dateFeat)
        results.append(r)
    print('completed')
    
    print(results)
    json_data = json.dumps(results, default=str)
    
    # for k in selectedColumns:
    #     print('starting')
    #     Dict= rulesforSingleColumnTest(df,k,conFeat,catFeat,dateFeat,NumericCorrMatrix,CateoricalCorrMatrix,refColumns, df_corr_sort)
    #     rules.append(Dict)
    # print('completed')
    # json_data = json.dumps(rules, default=str)

    return json_data




def getrules_mdmglobal(content):

    sourceId= content['sourceId']
    connectiondetails={}
    typeofcon=""
  
    rules=[]
    selectedColumns= content['selectedColumns']
    

    results=[]
    for k in selectedColumns:
        r=rulesforSingleColumn_mdmglobal(k)
        results.append(r)
    print('completed')
    
    print(results)
    json_data = json.dumps(results, default=str)
    
    # for k in selectedColumns:
    #     print('starting')
    #     Dict= rulesforSingleColumnTest(df,k,conFeat,catFeat,dateFeat,NumericCorrMatrix,CateoricalCorrMatrix,refColumns, df_corr_sort)
    #     rules.append(Dict)
    # print('completed')
    # json_data = json.dumps(rules, default=str)

    return json_data

def rulesforSingleColumn_mdm(df,selectedColumn,conFeat,catFeat,dateFeat):
    
    
    rules=[] 

    Dict = {}
    try:
        if selectedColumn!="":
            k=selectedColumn
            statisticAnal= []

            if k in conFeat:
                
                Dict['statistics'] ={}
            if k in catFeat:
                Dict['statistics'] ={}

            # 06/06/2022- Thiru #78 (New Proc to handle the date columns)
            if k in dateFeat:
                
                Dict['statistics'] ={}
            RulesDict = {}
            RulesDict1 = {}
            rules_list=[]
            if is_string_dtype(df[k]):
                # RulesDict1['rule'] ='DataType'
                # isAlphacount = df[k].str.isalpha().sum()
                # isAlphaNumcount= df[k].str.isalnum().sum() - isAlphacount

                # RulesDict1['value'] =  'Text'
                Dict['DataType']='Text'

                # RulesDict1['dimension'] =  'Validity'
                # RulesDict1['operator']  = 'Shouldbe'
                # RulesDict1['format']  = ''
                # rules_list.append(RulesDict1)

                RulesDict['rule'] ='Format'
                RulesDict['value'] =  'WWWDDD'                
                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'
                RulesDict['format']  = 'WWWDDD'
                rules_list.append(RulesDict)
                     

            if is_numeric_dtype(df[k]):
                Dict['DataType']='Numeric'
                # RulesDict1['rule'] ='DataType'
                # RulesDict1['value'] =  'Numeric'
                # RulesDict1['dimension'] =  'Validity'
                # RulesDict1['operator']  = 'Shouldbe'
                # RulesDict1['format']  = ''
                # rules_list.append(RulesDict1)

                RulesDict['rule'] ='Format'
                RulesDict['value'] =  '###.##'                
                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'
                RulesDict['format']  = '###.##' 
                rules_list.append(RulesDict)

            # 06/06/2022- Thiru #78 (New process to handle the date columns)
            if is_datetime64_any_dtype(df[k]):
                Dict['DataType']='Datetime'
                # RulesDict1['rule'] ='DataType'
                # RulesDict1['value'] =  'Datetime'
                # RulesDict1['dimension'] =  'Validity'
                # RulesDict1['operator']  = 'Shouldbe'
                # RulesDict1['format']  = ''
                # rules_list.append(RulesDict1)

                RulesDict['rule'] ='Format'
                RulesDict['value'] =  '%d-%m-%Y'                
                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'
                RulesDict['format']  = '%d-%m-%Y'
                rules_list.append(RulesDict)
                
               
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
        
    except Exception as e:
        return {}



def rulesforSingleColumn_mdmglobal(selectedColumn):
    
    
    rules=[] 

    Dict = {}
    try:
        if selectedColumn!="":
            k=selectedColumn
            statisticAnal= []


            RulesDict = {}
            RulesDict1 = {}
            rules_list=[]
            Dict['DataType']='Text'  
            RulesDict['rule'] ='ANY'
            RulesDict['value'] =  'NONEMPTY'                
            RulesDict['dimension'] =  ''
            RulesDict['operator']  = ''
            RulesDict['type']  = ''
            RulesDict['format']  = 'Non-empty Value'
            rules_list.append(RulesDict)

            RulesDict1['rule'] ='DIFF'
            RulesDict1['value'] =  'S1'                
            RulesDict1['dimension'] =  ''
            RulesDict1['type']  = ''
            RulesDict1['operator']  = ''
            RulesDict1['format']  = ''
            rules_list.append(RulesDict1)
                
               
            
            Dict['column'] =k
            Dict['rules'] = rules_list
            Dict['statistics']=[]
            rules.append(Dict)
        return Dict
        
    except Exception as e:
        return {}

def getRMaxId_mdm(sourceId):
        with open('dbmdatamodule.json', 'r') as openfile:
            json_object = json.load(openfile)
        data = json_object
        IDList=[]
        IDList.append('0')
        for obj in data['mdbSources']:
                if obj["sourceId"]==sourceId:
                   for obj1 in obj["rules"]:
                        IDList.append(obj1["rulesetId"][(len(sourceId)+1):] )
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))

def getRMaxId_mdmglobal(sourceId):
        with open('dbmdRules.json', 'r') as openfile:
            json_object = json.load(openfile)
        data = json_object
        IDList=[]
        IDList.append('0')
        for obj in data['mdbSources']:
                if obj["sourceId"]==sourceId:
                   for obj1 in obj["rules"]:
                        IDList.append(obj1["rulesetId"][(len(sourceId)+1):] )
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))

def CreateRulesetmdm(content):
    data={}
    
    sourceId=content['sourceId']
    newRules = content['ruleset']
    data=  json.loads(removeAEntityDB_mdb(sourceId))
    selectedMatchingKey=content['selectedMatchingKey']
    AnalysisList= data['mdbSources']

    rulesObject={}
    datarules= json.loads(GetAEntityDB_mdb(sourceId))
    datarulesList= datarules['mdbSources']
    datarulesList['selectedMatchingKey']=selectedMatchingKey
    settings=datarulesList['settings']
    settings['selectedMatchingKey']=selectedMatchingKey
    datarulesList['settings']=settings
    existingrulesList=[]
    existingrulesList=datarulesList['rules']
    rulesObject['rulesetId']=sourceId + 'R' + str(int(getRMaxId_mdm(sourceId))+1)
    rulesObject['selectedColumns'] = content['selectedColumns']
    rulesObject['startDate'] = content['startDate']
    rulesObject['endDate'] = content['endDate']
    #rulesObject['Referencepath'] = content['Referencepath']
    rulesObject['rulesetName']= content['rulesetName']
    rulesObject['ruleset']=newRules
    content['rulesetId']=rulesObject['rulesetId']
    existingrulesList.append(rulesObject)

    datarulesList['rules'] =existingrulesList
    sourcelogs=datarulesList['sourcelogs']
    level2filepath=sourcelogs['level2filepath']
    sourceFilename2='std_'+level2filepath
    sourcelogs['level2Stdfilepath']=sourceFilename2
    datarulesList['sourcelogs']=sourcelogs
    content['sourcelogs']=sourcelogs
    content['settings']=settings
    AnalysisList.append(datarulesList)
    data['mdbSources'] = AnalysisList
    json.dump(data, open("dbmdatamodule.json","w"), default=str)
    
    
    if settings["vendorName"]!= "":
            vendorName=settings["vendorName"]
           
    
    sandardizebyRuleid(level2filepath,sourceId,newRules,[],sourceFilename2,vendorName)
    return json.dumps(content, default=str)

def CreateRulesetmdm_globaldefault(sourceId):
    data={}
    print('inside default')
    data=  json.loads(removeAEntityDB_mdbglobal(sourceId))
    
    AnalysisList= data['mdbSources']

    rulesObject={}
    datarules= json.loads(GetAEntityDB_mdbglobal(sourceId))
    datarulesList= datarules['mdbSources']
    
    if not datarulesList:
        datarulesList['sourceId']=sourceId
        datarulesList['rules']=[]
        datarulesList['GoldenSourcePath']=''
        print('inside default ave')
        AnalysisList.append(datarulesList)
        data['mdbSources'] = AnalysisList
        json.dump(data, open("dbmdRules.json","w"), default=str)
    
    
    return 'true'

def CreateRulesetmdm_global(content):
    data={}
    
    sourceId=content['sourceId']
    newRules = content['ruleset']
    data=  json.loads(removeAEntityDB_mdbglobal(sourceId))
    
    AnalysisList= data['mdbSources']

    rulesObject={}
    datarules= json.loads(GetAEntityDB_mdbglobal(sourceId))
    datarulesList= datarules['mdbSources']
    
    
    existingrulesList=[]
    if 'rules' in datarulesList:
        existingrulesList=datarulesList['rules']
    rulesObject['rulesetId']=sourceId + 'R' + str(int(getRMaxId_mdmglobal(sourceId))+1)
    rulesObject['selectedColumns'] = content['selectedColumns']
    rulesObject['startDate'] = content['startDate']
    rulesObject['endDate'] = content['endDate']
    #rulesObject['Referencepath'] = content['Referencepath']
    rulesObject['rulesetName']= content['rulesetName']
    rulesObject['ruleset']=newRules
    content['rulesetId']=rulesObject['rulesetId']
    existingrulesList.append(rulesObject)

    datarulesList['rules'] =existingrulesList
    datarulesList['sourceId']=sourceId
    datarulesList['GoldenSourcePath']=sourceId+'_golden.csv'
    content['GoldenSourcePath']=sourceId+'_golden.csv'
    AnalysisList.append(datarulesList)
    data['mdbSources'] = AnalysisList
    json.dump(data, open("dbmdRules.json","w"), default=str)
    
    sourcepath=[]
    sourcepath= getlistfromMDB_byvendor(sourceId)

    #sourcepath.append('std_BB_STD_S1BB Bloomberg Data.csv')
    #sourcepath.append('std_RE_STD_S2RE Reuters Data.csv')
    matchingkey='ISIN'
    ruleset=newRules
    path=sourceId+'_golden.csv'
    Availablecolumns=['PRIMARY ID','ISIN','EXCHANGE',
                    'NAME','INDUSTRY SECTOR',
                    'INDUSTRY GROUP','CURRENT MARKET CAP',
                    'LAST PRICE',
                    'NAME OF THE CEO',
                    'START DATE OF CEO',
                    'CEO COMPENSATION',
                    'COMPANY TELEPHONE NUMER',
                    'COMPANY WEB ADDRESS']
    sourceids=[]                
    getgoldencopy(sourceids,sourcepath,matchingkey,ruleset,path,Availablecolumns)
    return json.dumps(content, default=str)

def getgoldencopy(sourceids,sourcepath,matchinkey,ruleset,path,Availablecolumns):
    s1path=sourcepath[0]
    print(s1path)
    S1=df_from_path(s1path)
    s2path=sourcepath[1]
    S2=df_from_path(s2path)
    print(S1.head())
    print(S2.head())
    comparison = S1.merge(S2, how='outer', on=matchinkey)
    merged=comparison
    print(merged.head())
    for r in ruleset:
            col=r['column']
            KeyName=col 
            if col!=matchinkey:
                
                for ru in r['rules']:
                    if ru['rule'] =='ANY':
                        if 'value' in ru:
                            ComValue= ru['value']
                        else:
                            ComValue='NONEMPTY'    
                        
                        if ComValue=='NONEMPTY' :
                            merged[col] = merged[col+'_x'] 
                            merged[col]= np.where(merged[col].isnull(),merged[col+'_y'] , merged[col] )
                        elif ComValue=='S1':
                            merged[col] = merged[col+'_x'] 
                        elif ComValue=='S2':
                            merged[col] = merged[col+'_y']     
                    if ru['rule'] =='DIFF':
                        ComValue= ru['value'] 
                        if ComValue=='S1':
                            merged[col] = merged[col+'_x'] 
                        elif ComValue=='S2':
                            merged[col] = merged[col+'_y']
    merged = merged[Availablecolumns]
    merged.to_csv(path,  index=False)
    return ''     



def sandardizebyRuleid(sourcepath,sourceid,ruleset,cdecolumns,sourceFilename2,vendorName):
    df=df_from_path(sourcepath)
    print(df.columns.tolist())
    df.columns = df.columns.str[3:]#str.lstrip(vendorName+"_")
    print(df.columns.tolist())
    print(df.head())
    if not df.empty:
        df2=standardizeDFbyRules(df,ruleset,cdecolumns)
        df2.to_csv(sourceFilename2,  index=False)
        return 'true'

def editRulesetmdm(content):
    data={}
    
    sourceId=content['sourceId']
    newRules = content['ruleset']
    rulesetId= content['rulesetId']

    data=  json.loads(removeAEntityDB_mdb(sourceId))
    AnalysisList= data['mdbSources']

    rulesObject={}
    datarules= json.loads(GetAEntityDB_mdb(sourceId))
    datarulesList= datarules['mdbSources']
    existingrulesListedited=[]
    for obj1 in datarulesList['rules']:
        if obj1["rulesetId"]!=rulesetId:
            existingrulesListedited.append(obj1)
    rulesObject['selectedColumns'] = content['selectedColumns']
    rulesObject['startDate'] = content['startDate']
    rulesObject['endDate'] = content['endDate']
    rulesObject['rulesetName']= content['rulesetName']
    rulesObject['ruleset']=newRules
    rulesObject['rulesetId']=rulesetId
    existingrulesListedited.append(rulesObject)

    datarulesList['rules'] =existingrulesListedited

    AnalysisList.append(datarulesList)
    data['mdbSources'] = AnalysisList
    json.dump(data, open("dbmdatamodule.json","w"), default=str)
    return json.dumps(content, default=str)



def editRulesetmdmglobal(content):
    data={}
    
    sourceId=content['sourceId']
    newRules = content['ruleset']
    rulesetId= content['rulesetId']

    data=  json.loads(removeAEntityDB_mdbglobal(sourceId))
    AnalysisList= data['mdbSources']

    rulesObject={}
    datarules= json.loads(GetAEntityDB_mdbglobal(sourceId))
    datarulesList= datarules['mdbSources']
    existingrulesListedited=[]
    for obj1 in datarulesList['rules']:
        if obj1["rulesetId"]!=rulesetId:
            existingrulesListedited.append(obj1)
    rulesObject['selectedColumns'] = content['selectedColumns']
    rulesObject['startDate'] = content['startDate']
    rulesObject['endDate'] = content['endDate']
    rulesObject['rulesetName']= content['rulesetName']
    rulesObject['ruleset']=newRules
    rulesObject['rulesetId']=rulesetId
    existingrulesListedited.append(rulesObject)

    datarulesList['rules'] =existingrulesListedited

    AnalysisList.append(datarulesList)
    data['mdbSources'] = AnalysisList
    json.dump(data, open("dbmdRules.json","w"), default=str)
    return json.dumps(content, default=str)

def execNumericOperations(df,operation,paramContent):
    return True


def savesource(df,sourceFilename,sourcevendorPrefix):
        content={}
        if not df.empty:
            
                if sourceFilename != '':
                            
                        df = df.add_prefix(sourcevendorPrefix+'_')
                        if any ('Unnamed' in col for col in df.columns.values.tolist()):
                                    print('Missing Header Columns')
                                    raise pd.errors.ParserError 
                        sourceFilename1=sourcevendorPrefix+'_'+sourceFilename
                        df.to_csv(sourceFilename1, index=False)

                result={}
                result['level1filepath']=sourceFilename1
                sourceFilename2=sourcevendorPrefix+'_'+'STD_'+sourceFilename
                if sourcevendorPrefix=="BB":
                    df.rename(columns = {'BB_TICKER':'BB_PRIMARY ID','BB_ISIN':'BB_ISIN','BB_EXCHANGE':'BB_EXCHANGE',
                    'BB_NAME':'BB_NAME',   
                    'BB_INDUSTRY_SECTOR':'BB_INDUSTRY SECTOR','BB_INDUSTRY_GROUP':'BB_INDUSTRY GROUP',
                    'BB_CUR_MKT_CAP':'BB_CURRENT MARKET CAP',
                    'BB_PX_LAST':'BB_LAST PRICE',
                    'BB_NAME_OF_CEO':'BB_NAME OF THE CEO',
                    'BB_CEO_START_DATE':'BB_START DATE OF CEO',
                    'BB_TOT_CEO_COMP':'BB_CEO COMPENSATION',
                    'BB_COMPANY_TEL_NUMBER':'BB_COMPANY TELEPHONE NUMER',
                    'BB_COMPANY_WEB_ADDRESS':'BB_COMPANY WEB ADDRESS'}, inplace = True)
                    df.to_csv(sourceFilename2, index=False)
                    result['level2filepath']=sourceFilename2
                    result['level2Stdfilepath']=sourceFilename2
                if sourcevendorPrefix=="RE":
                    df.rename(columns = {'RE_EXCH':'RE_EXCHANGE','RE_RIC':'RE_PRIMARY ID','RE_ISIN ID':'RE_ISIN',
                                        'RE_NAME':'RE_NAME','RE_SECTOR':'RE_INDUSTRY SECTOR',
                                        'RE_GROUP':'RE_INDUSTRY GROUP',
                                        'RE_MARKET_PRICE':'RE_CURRENT MARKET CAP',
                                        'RE_LAST_PRICE':'RE_LAST PRICE',
                                        'RE_CEO_NAME':'RE_NAME OF THE CEO',
                                        'RE_CEO_START_DT':'RE_START DATE OF CEO',
                                        'RE_TOT_CEO_COMP':'RE_CEO COMPENSATION',
                                        'RE_COMP_TEL_NUMBER':'RE_COMPANY TELEPHONE NUMER',
                                        'RE_COMP_WEB_ADDRESS':'RE_COMPANY WEB ADDRESS'}, inplace = True)
                    df.to_csv(sourceFilename2,  index=False)
                    result['level2filepath']=sourceFilename2
                    result['level2Stdfilepath']=sourceFilename2
        return result

def getMasterDMview(sourcelogs,settings): 
    
    level2filepath=sourcelogs['level2filepath']
    level2Stdfilepath=sourcelogs['level2Stdfilepath']
    df1=df_from_path(level2filepath)
    df2=df_from_path(level2Stdfilepath)
    df3=df_from_path(level2Stdfilepath)
    df1 = df1.replace(np.nan, "None", regex=True)
    df2 = df2.replace(np.nan, "None", regex=True)
    df3 = df3.replace(np.nan, "None", regex=True)
    #df1=df1.head()
    
    viewList={}
    
    # print(df1l)
    # df2=df2.head()
    if settings["vendorName"]!= "":
            vendorName=settings["vendorName"]
           
            df1.columns = df1.columns.str[3:]#str.lstrip(vendorName+"_")
            
            #df2.columns = df2.columns.str[3:]#str.lstrip(vendorName+"_")
            #df3.columns = df3.columns.str[3:]#str.lstrip(vendorName+"_")
    df2l=df2.to_dict(orient='records')
    df1l=df1.to_dict(orient='records')
    # print(df2l)
    viewList["BasicLayer"]=df1l
    # df3=df3.head()
    viewList["StandardLayer"]=df2l
    df3l=df3.to_dict(orient='records')
    
    viewList["Consolidated layer"]=df3l
    return viewList

def getMasterGOldenview(sourcelogs): 
    
    file1path=sourcelogs['file1']
    file2path=sourcelogs['file2']
    file3path=sourcelogs['golden']
    df1=df_from_path(file1path)
    df2=df_from_path(file2path)
    df3=df_from_path(file3path)
    df1 = df1.replace(np.nan, "None", regex=True)
    df2 = df2.replace(np.nan, "None", regex=True)
    df3 = df3.replace(np.nan, "None", regex=True)
    #df1=df1.head()
    
    viewList={}
    
    
    df2l=df2.to_dict(orient='records')
    df1l=df1.to_dict(orient='records')
    # print(df2l)
    viewList["file1"]=df1l
    # df3=df3.head()
    viewList["file2"]=df2l
    df3l=df3.to_dict(orient='records')
    
    viewList["golden"]=df3l
    return viewList

def standardizeDFbyRules(df2,ruleset,cdecolumns):
    print('starting long running task')
    rules=[]
    rulesObject={}
    resultsetDictlist=[]    
    framenew= df2.copy(deep=True)
       
    count=0
    try:
        #for KeyName in cdecolumns:
        for r in ruleset:
            col=r['column']
            KeyName=col
           
            print(KeyName)

            resultsetDictlist=[]
            Dict = {}
            
            
            colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
            

            
           
            frametbp= framenew.copy(deep=True)

            if not frametbp.empty:
                
                for ru in r['rules']:
                    if ru['rule'] =='Format':
                        ComValue= ru['value']
                        #framenew = framenew.dropna()
                        oper=ru['operator']
                        
                        if oper=='trim' :
                                framenew[col] = framenew[col].replace(np.nan, "None", regex=True)
                                #framenew = framenew[(framenew[col].astype(str))==(ComValue)]
                                #framenew = framenew[col].str.strip()
                                framenew[col] = framenew[col].apply(lambda x: x.strip())
                        elif oper=='toUpperCase':
                                framenew[col] = framenew[col].replace(np.nan, "None", regex=True)
                                framenew[col] = framenew[col].apply(lambda x: x.upper())
                        elif oper=='toLowerCase':
                                framenew[col] = framenew[col].replace(np.nan, "None", regex=True)
                                framenew[col] = framenew[col].apply(lambda x: x.lower())
                        elif oper=='substring':
                                framenew[col] = framenew[col].replace(np.nan, "None", regex=True)
                                framenew[col] = framenew[col].str.strip()
                        elif oper=='concat':
                                framenew[col] = framenew[col].replace(np.nan, "None", regex=True)
                                framenew[col] = framenew[col].str.strip()
                        elif oper=='replace':
                            framenew[col] = framenew[col].replace(np.nan, "None", regex=True)
                            framenew[col] = framenew[col].str.strip()
                        elif oper=='toString':
                            framenew[col] = framenew[col].replace(np.nan, "None", regex=True)
                            framenew[col] = framenew[col].astype('str')
                    if ru['rule']=='Transformation':                        
                        oper=ru['operator']
                        formula= ru['value']
                        
                        formulaText=""
                        for valcol in formula:
                            formulaText=valcol['formulaText']
                        print(formulaText)    
                        if formulaText!="":    
                            framenew.columns = framenew.columns.map(lambda x: x.replace(' ', '__')) 
                            formulaText=formulaText.replace(' ==','=')   
                            formulaText=formulaText.replace(' ','__')
                            print(formulaText)
                            print('in')
                            framenew.eval(formulaText, inplace=True)
                            print('out')
                            framenew.columns = framenew.columns.map(lambda x: x.replace('__', ' '))
        print (framenew.head())    
        return framenew    

    except Exception as e:
        resultobject={}
        
        resultobject['results']=rules
        resultobject['aggResults']={}
        resultobject['isException']='Yes'
        resultobject['exception']=  str(e)
        print('exception')
        print(str(e))
        return pd.DataFrame()
