from flask import Blueprint,request, render_template, send_from_directory
from flask_cors import CORS
import pandas as pd
import json
import os
import os.path
from io import StringIO
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_datetime64_any_dtype
from werkzeug.utils import secure_filename
from module_plot import *
from datetime import date
import datetime
import threading
import multiprocessing as mp
import time

import numpy as np
from ClassifyColumns import classify_columns
from sendMail import sendMailtoUser
from pathlib import Path
from common import *
from sqlalchemy import create_engine, false, true

import cx_Oracle
module_dqm = Blueprint('module_dqm',__name__)

from multiprocessing import freeze_support

@module_dqm.route('/api/uploadSource', methods=['POST'])
def uploadSourceoracle():
    uploaded_files = request.files.getlist("file[]")
    #print(uploaded_files)
    content =json.loads(request.form.get('data'))
    sourceId= content['sourceId']
    if "uploadSourceId" in content:
        uploadSourceId= content['uploadSourceId']
    else:
        uploadSourceId=''    
    rulesetId= content['rulesetId']
    isMultiSource= content['isMultiSource']
    multiSourceKey= content['multiSourceKey']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['sourceObj']
    settings= content['settings']
    multiSourceColumn= settings["multiSourceColumn"]
    #print(multiSourceColumn)

    # as part of data cleaned files integration
    if 'isCleanedSource' in content:
        isCleanedSource= content['isCleanedSource'] # YES /NO
        cleanedSourcePath= content['cleanedSourcePath']
        if isCleanedSource== "YES":
            uploadReason="cleansedsource"
        #print(isCleanedSource)
    else:
        isCleanedSource= 'NO'
        cleanedSourcePath= ''
        #print(isCleanedSource)
    
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
    if 'isCleanedSource' in content:
        isCleanedSource= content['isCleanedSource'] # YES /NO
        cleanedSourcePath= content['cleanedSourcePath']
        newuploadId=sourceId+'U'+ str(int(getUploadMaxId(sourceId))+1)        
    sourcename = os.path.splitext(sourceFilename)[0]
    #print(sourcename)
    sourceFilename=newuploadId + sourcename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB(sourceId))
    #print('launch entity in upload source oracle')
    
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
                            #print('Missing Header Columns')
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
                            #print('Missing Header Columns')
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
                                    #print('Missing Header Columns')
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

    #print(uploadReason)
    if(uploadReason==''):
        if(isMultiSource=="Yes"):
            if(settings['frequency']=="DAILY"):
                
                multiSourceKeyList = []
                multiSourceKeyList.append(multiSourceKey)
                #print(df[multiSourceColumn].unique())
                #print(multiSourceKeyList)

                for item in uploadsObject:
                        multisrObj= item['multiSourcePath']
                        if df[multiSourceColumn].unique() == multiSourceKeyList:
                            payloadDate =  datetime.datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
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
                            payloadDate =  datetime.datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
    else:
        if uploadReason!="cleansedsource":
            if(isMultiSource!="Yes"):
                #print("in")
                if(settings['frequency']=="Daily"):
                    #print("in one")
                    editedUploadObj=[]
                    for item in uploadsObject:
                                #print("in two")
                                payloadDate =  datetime.datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                                actualDate = datetime.datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
                                if payloadDate.date()!=actualDate.date():
                                    #print("in three")                             
                                    editedUploadObj.append(item)
                    uploadsObject= editedUploadObj
                    #print("in four")    
                    #print(uploadsObject)         

    # df= df_from_path(mpath)                       


    resultContent={}

    resultContent['launchId']=newuploadId
    resultContent['uploadId']=newuploadId
    resultContent['uploadSourceId']=uploadSourceId
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
    content['uploadSourceId']=uploadSourceId
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
        #print("Starting long task")
        #print("Your params:", inputcontent)
        rules=[]
        time.sleep(1)
        sourceId = inputcontent['sourceId']
        rulesetId = inputcontent['rulesetId']
        KeyName = inputcontent['keyname']
        uploadId=inputcontent['uploadId']
        #LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId)
        processSourcethread(sourceId,rulesetId,KeyName,uploadId,uploadDate,sourcedetails,settings)
        #print('Completed the main thread function')
    tempDict['stuats'] ='started'
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': inputcontent1})
    thread.start()

    return json.dumps(tempDict, default=str)

def processSourcethread(sourceId,rulesetId,KeyNameList,uploadId,uploadDate,sourcedetails,settings):
    #print('starting long running task')
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
    #print(LaunchEntityRaw)
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
        #print(sourcepath)
        df= df_from_path_uploadSource(sourcepath)
        

    df2 = pd.DataFrame(df, columns=cdecolumns)
    #df2 = df1.where(pd.notnull(df1), 'None')
    LaunchEntity['AnalysisResultList']='R_'+(uploadId)
    LaunchEntity['launchMaxId']=uploadId + 'L' + str(len(KeyNameList)+1)
    #print('saving main results')
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
    #    #print('joining results')
    #print('checking results')

    results = [q.get() for j in jobs]
    testResultList=[]
    testResultList=testResultList+(results)
    
    completeness=[]
    Accuracy=[]
    Uniqueness=[]
    Integrity=[]
    Validity=[]
    for finalResults in testResultList:
        #print(finalResults)
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
    MAiluserMsgList=[]
    #for name in AnalysisObj["dataOwner"]:
    with open('userdb.json', 'r') as openfile:
            json_object_user = json.load(openfile)

    if AnalysisObj["dataOwner"]!="":
        userMsg={}
        userMsg=getUsersFromownername(uploadDate ,AnalysisObj["sourceDataName"],AnalysisObj["dataOwner"],json_object_user)
        
        userMsgList=userMsgList+(userMsg)
        
    if "dataUser" in AnalysisObj:
        userMsgList=userMsgList+getUsersFromCategory(json_object_user,AnalysisObj["dataUser"],DictFinal,uploadDate,AnalysisObj["sourceDataName"],AnalysisObj["dataOwner"])
        print(userList)
        MAiluserMsgList=userMsgList
    if "dataProcessingOwner" in AnalysisObj:
        userMsgList=userMsgList+getUsersFromCategory(json_object_user,AnalysisObj["dataProcessingOwner"],DictFinal,uploadDate,AnalysisObj["sourceDataName"],AnalysisObj["dataOwner"])
       
    #if "dataSteward" in AnalysisObj:
        #message="The file is uploaded for "+ uploadDate +" and the results are available in DashBoard for the source Named "+AnalysisObj["sourceDataName"]
        #userMsgList=userMsgList+getUserNamesFromCategory(json_object_user,AnalysisObj["dataSteward"],message,AnalysisObj["sourceDataName"])
        
        


    receiver_email = "suji.arumugam@gmail.com"
    # message="The file is uploaded for "+ uploadDate +" and the results are available in DashBoard for the source Named "+AnalysisObj["sourceDataName"]
    # #print(userMsgList)
    addNotificationList(userMsgList ,"Active","UnRead")

    
    sendMailtoUserList(userMsgList)
    return 'Completed'

def addNotificationList(userNameList,  status,isRead):
    checkList=[]
    for usermsg in userNameList:
        addNotification(usermsg["user"],usermsg["msg"], status,isRead)
    return "true"

def sendMailtoUserList(userMsgList):
    print("checking the email list")
    print(userMsgList)
    
    checkList=[]
    #userMsgList = list(set(userMsgList))
    for usermsg in userMsgList:
        if usermsg["ActualResult"]==[]:
            print("Message is held") 
            #SendResultByMail(usermsg["fileName"],usermsg["ActualResult"],usermsg["TargetResult"],"suji.arumugam@gmail.com", 
            #usermsg["email"],"email_success_Owner.html",usermsg["fileName"],usermsg["subject"],"NO")
        else:     
            SendResultByMail(usermsg["fileName"],usermsg["ActualResult"],usermsg["TargetResult"],"suji.arumugam@gmail.com", 
                              usermsg["email"],usermsg["template"],usermsg["fileName"],usermsg["subject"],"YES")
            #SendResultByMail(usermsg["msg"], usermsg["email"],usermsg["Name"])
    return "true"



def getUsersFromCategory(json_object,category,aggFinalResults,uploadDate,sourceName,dataowner)   :
    userMsgList=[]
    userCategoryList=[]
    data={}
    owner=""
    
    
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
                    userMsg["ActualResult"]=aggFinalResults
                    userMsg["TargetResult"]=cat['tolerance']
                    userMsg["email"]= obj['email']
                    userMsg["Name"]=obj['name']
                    userMsg["fileName"]=sourceName
                    userMsg["template"]="email_success_User.html"
                    userMsg["subject"]="The file "+sourceName + " has passed the DQ checks and ready to be used"
                    userMsgList.append(userMsg)

                    # userMsg2={}
                    # userMsg2["user"]=obj["userName"]
                    # message="The file is uploaded for "+ uploadDate +" and the results are available in DashBoard for the source Named "+sourceName
                    # userMsg2["msg"]= message
                    # userMsg2["ActualResult"]=aggFinalResults
                    # userMsg2["TargetResult"]=cat['tolerance']
                    # userMsg2["email"]= obj['email']
                    # userMsg2["Name"]=obj['name']
                    # userMsg2["fileName"]=sourceName
                    # userMsg2["template"]="email_success_owner.html"
                    # userMsg2["subject"]="The file "+sourceName + " has passed the DQ checks and ready to be used for the tolerance for the data User " + cat['name']
                    # userMsgList.append(userMsg2)


                else:
                    userCategoryList.append(owner)
                    userMsg3={}
                    userMsg3=getUsersFromownernamewithTolerance(uploadDate,sourceName,dataowner,json_object,cat['tolerance'],aggFinalResults,cat) 
                    userMsgList.append(userMsg3)

                    userMsg1={}
                    userMsg1["user"]=obj["userName"]
                    message="The file which is uploaded for "+ uploadDate +" and corresponding to the source Named "+sourceName +" doesn't meet with the tolerance of the usercategory " + cat['name']
                    userMsg1["msg"]= message
                    userMsg1["ActualResult"]=aggFinalResults
                    userMsg1["TargetResult"]=cat['tolerance']
                    userMsg1["email"]= obj['email']
                    userMsg1["Name"]=obj['name']
                    userMsg1["fileName"]=sourceName
                    userMsg1["template"]="email_failure_user.html"
                    userMsg1["subject"]="The file "+sourceName + " has failed the DQ checks and not yet ready to be used"
                    userMsgList.append(userMsg1)
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
                    userMsg["fileName"]=sourceName
                    userMsg["ActualResult"]=[]
                    userMsg["TargetResult"]=[]
                    userMsg["template"]="email_success_owner.html"
                    userMsg["subject"]="The file "+sourceName + "has passed the DQ checks and ready to be used"
                    userMsgList.append(userMsg)
    return userMsgList

def getUsersFromownernamewithTolerance(uploadDate,sourceName,dataowner,json_object,category,aggFinalResults,cat)   :
    userMsgList=[]
    userCategoryList=[]
    data={}
    owner=""
    print("name is" + dataowner)
    
    userMsg={}
    
    data=json_object
    for obj in data['UserList']:
            if( dataowner in obj["name"]):
                    
                    userMsg["user"]=obj["userName"]
                    message="The file which is uploaded for "+ uploadDate +" and corresponding to the source Named "+sourceName +" doesn't meet with the tolerance of the usercategory " + cat['name']
                    userMsg["msg"]= message
                    userMsg["email"]= obj['email']
                    userMsg["Name"]=obj['name']
                    userMsg["fileName"]=sourceName
                    userMsg["ActualResult"]=aggFinalResults
                    userMsg["TargetResult"]=category
                    userMsg["email"]= obj['email']
                    userMsg["Name"]=obj['name']
                    userMsg["fileName"]=sourceName
                    userMsg["template"]="email_failure_owner.html"
                    userMsg["subject"]="The file "+sourceName + " has failed the DQ checks and not yet ready to be used with the tolerance of the usercategory " + cat['name']
    print("the result is")
    print(userMsg)                
    return userMsg

def getUserNamesFromCategory(json_object,category,message,sourceName)   :
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
                    userMsg["fileName"]=sourceName
                    userMsg["ActualResult"]=[]
                    userMsg["TargetResult"]=[]
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


def addAggResults(sourceId,sourcedetails,launchId, uploadId,rulesetId,uploadDate,uploadTime,aggResults,sourcedataName,sourcedataDesc,stype,settings):
    #print(sourceId,launchId, uploadId,rulesetId,uploadDate,uploadTime,aggResults)
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
    x = datetime.datetime.now()
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
         
def processSourceByKeyName(df2,KeyName,launchNewId,ruleset,cdecolumns,ReferenceObj,uploadId,q):
    #print('starting long running task')
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
    #print("testing value")
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
            #print("1111")
            if isOutlier=='true':
                #print("1112")
                Dict['isOutlier'] =checkoutlierforkeyvalue(dtypevalue,KeyName)
                #print("1113")
            else:
                Dict['isOutlier'] = 'false'
                #print("1114")
            Dictconstraints = {}
            '''s1=(len(frame))

            s2= frame.isnull().sum().sum()
            v2=(s2/s1)*100
            v1=100-round(v2,2)'''
            #print('KeyName')
            #print(KeyName)
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
            DNull['Duplicates'] = str(farmenew.duplicated().sum())
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
                            #print(file_ext)
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
                            #print(querystring)
                            #print(invertedQString)
                            #print(col)
                            #print('conditional vala main loop')

                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                #print('actual complie with formule: ')
                                #print( num_rows)
                                idf2= framenew.query(invertedQString)
                                x = pd.concat([df2, idf2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                #print(r7.head())
                                num_rows= len(framecopy[col])-len(r7)
                                #print('actual count with formule: ')
                                #print( num_rows)
                                ##print(len(df3))
                                ##print((df3.index).tolist())
                            rulename=querystring
                        else:
                            querystring= formulaText#ru['formulaText']
                            formula= ru['value']
                            #querystring =GetQueryStringnew(formula)
                            #invertedQString=GetinvertQueryString(formula)
                            invertedQString=querystring
                            framecopy= framenew.copy(deep=True)
                            #print(querystring)
                            #print(invertedQString)
                            #print(col)
                            #print('conditional vala else loop')
                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                #print('actual complie with formule: ')
                                #print( num_rows)
                                
                                x = pd.concat([framenew, df2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                #print('errror here')
                                #print(len(r7))
                                #print(len(framenew[col]))
                                num_rows= len(framenew[col])-len(r7)
                                #print('actual count not equla to formule: ')
                                #print( num_rows)
                                ##print(len(df3))
                                ##print((df3.index).tolist())
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
                #print(IntegValue)
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
                #print('inside accuracy loop')
                #print(Accvalue)
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
                #print(Dict)
            rules.append(Dict)
            #print('calling the saveresultset to s3')

            saveResultsets(resultsetDictlist,(launchNewId)+str(KeyName))
        # #print('calling the saveresultset to s3')

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
        #print('saving main result set')
        saveResultsets(fResult,'R_'+(uploadId)+ keyv)
        #print('saving main result set as' + 'R_'+(uploadId)+ keyv)
        #print(collistdetails)

        q.put(DictFinal)
    except Exception as e:
        resultobject={}
        resultobject['launchId']=launchNewId
        resultobject['keyName']=keyv
        resultobject['results']=rules
        resultobject['aggResults']={}
        resultobject['isException']='Yes'
        resultobject['exception']=  str(e)
        #print(str(e))
        fResult={}
        fResult['AnalysisResultList']=resultobject
        saveResultsets(fResult,'R_'+(uploadId)+ keyv)
        q.put({})


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
                        #print(condn["value"])
                        #print('numeric')
                        querystring= querystring + condn["value"]
                    else:
                        #print(condn["value"])
                        #print('non numeric')
                        querystring= querystring + "'"+condn["value"] + "'"
                if( condn["condition"]!=""):
                    if(condn["condition"]=="AND"):
                        querystring= querystring +"&"
                    elif(condn["condition"]=="OR"):
                        querystring= querystring +"|"
                else:
                    ##print(condn["operator2"])
                    if ( condn["operator2"]!=""):

                        querystring= querystring + condn["operator2"]
                    else:
                        querystring= querystring + ')'
                if( condn["group"]):
                        querystringg= GetQueryStringGroupnew(formula)
                        #print(querystringg)
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
                            #print(listGroup["value"])
                            #print('numeric')
                            querystring= querystring + listGroup["value"]
                        else:
                            #print(listGroup["value"])
                            #print('non numeric')
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
                        #print(condn["value"])
                        #print('numeric')
                        querystring= querystring + condn["value"]
                    else:
                        #print(condn["value"])
                        #print('non numeric')
                        querystring= querystring + "'"+condn["value"] + "'"
                if(condn["condition"]!=""):
                    if(condn["condition"]=="AND"):
                        querystring= querystring +"&"
                    elif(condn["condition"]=="OR"):
                        querystring= querystring +"|"
                else:
                    #print(condn["operator2"])
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

def getNotificationMaxId(notificationDict):
    if "Notification" in notificationDict:
        existinglsit= notificationDict["Notification"]
    idList=[]
    idList.append(0)
    for notifyList in existinglsit:
        idList.append(notifyList["Id"])
    dli2 = [int(s) for s in idList]
    return (str(max(dli2)+1))

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
                        #print(condn["value"])
                        #print('numeric')
                        querystring= querystring + condn["value"]
                    else:
                        #print(condn["value"])
                        #print('non numeric')
                        querystring= querystring + "'"+condn["value"] + "'"
                if( condn["condition"]!=""):
                    if(condn["condition"]=="AND"):
                        querystring= querystring +"&"
                    elif(condn["condition"]=="OR"):
                        querystring= querystring +"|"
                else:
                    ##print(condn["operator2"])
                    if ( condn["operator2"]!=""):

                        querystring= querystring + condn["operator2"]
                    else:
                        querystring= querystring + ')'
        querystring=querystring + ')'

    return querystring


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



def getUploadCleanMaxId(sourceId,sourceUploadID):
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
