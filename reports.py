
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
import re
from CorrelationFunctions import compute_associations
from pathlib import Path

reports = Blueprint('',__name__)

@reports.route("/api/getRules_Reports", methods=["POST"])
def getrules_reports():
    print("")
    content = request.get_json()
    sourcepath= content['sourcepath']
    connectiondetails={}
    typeofcon=""
    df= df_from_path(sourcepath)    
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


    results=[]
    for k in selectedColumns:
        r=rulesforSingleColumn_reports(df,k)
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


@reports.route("/api/getcatalogueRules_Reports", methods=["POST"])
def getrules_reportsbyCatalogue():
    content = request.get_json()
    #sourcepath= content['sourcepath']
    sourcepath="S1Titanic.xls"
    connectiondetails={}
    typeofcon=""
    df= df_from_path(sourcepath)    
    rules=[]
    selectedColumns= content['selectedColumns']
    
    
    selectedColumns=getcatalogueforterms(selectedColumns)
    print(selectedColumns)
    
    
    df_meta = pd.DataFrame(df.dtypes).reset_index()
    df_meta.columns = ["column_name", "dtype"]
    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")

    conFeat = df_meta[df_meta.dtype == "int64"].column_name.tolist() + df_meta[df_meta.dtype == "float64"].column_name.tolist()
    catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
    dateFeat = df_meta[df_meta.dtype == "datetime64[ns]"].column_name.tolist() + df_meta[df_meta.dtype == "datetime"].column_name.tolist()


    results=[]
    for kdetail in selectedColumns:
        k=kdetail['Column']
        kid=kdetail['id']
        kterm=kdetail['BusinessTerm']
        r=rulesforSingleColumn_reports(df,k,kid,kterm)
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



def rulesforSingleColumn_reports(df,selectedColumn,kid,kterm):
    
    
    

    Dict = {}
    try:
        if selectedColumn!="":
            k=selectedColumn
            statisticAnal= []

            RulesDict = {}
            RulesDict1 = {}
            rules_list=[]
            catFeat=[]
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
                    
            if is_string_dtype(df[k]):
                
                Dict['DataType']='Text'
                Dict['id']=kid
                Dict['BusinessTerm']=kterm
                RulesDict['rule'] ='LengthStatistics'
                RulesDict['value'] =  [        "Min",        "Median",        "Average",        "Max"      ]                
                RulesDict['dimension'] =  ''
                RulesDict['operator']  = ''
                RulesDict['format']  = ''
                RulesDict["type"]  = "LineChart"
                rules_list.append(RulesDict)

                

            if is_numeric_dtype(df[k]):
                Dict['DataType']='Numeric'
                Dict['id']=kid
                Dict['BusinessTerm']=kterm
                RulesDict['rule'] ='ValueStatistics'
                RulesDict['value'] =  [        "Min",        "Median",        "Average",        "Max"      ]                
                RulesDict['dimension'] =  ''
                RulesDict['operator']  = ''
                RulesDict['format']  = ''
                RulesDict["type"]  = "LineChart"
                rules_list.append(RulesDict)
                
                RulesDictl={}
                RulesDictl['rule'] ='LengthStatistics'
                RulesDictl['value'] =  [        "Min",        "Median",        "Average",        "Max"      ]                
                RulesDictl['dimension'] =  ''
                RulesDictl['operator']  = ''
                RulesDictl['format']  = ''
                RulesDictl["type"]  = "LineChart"
                rules_list.append(RulesDictl)

                RulesDictout={}     
                RulesDictout['rule'] ='Outliers'
                RulesDictout['value'] =  ''              
                RulesDictout['dimension'] =  ''
                RulesDictout['operator']  = ''
                RulesDictout['format']  = ''
                RulesDictout["type"]  = "BarChart"
                rules_list.append(RulesDictout)
            # 06/06/2022- Thiru #78 (New process to handle the date columns)
            if is_datetime64_any_dtype(df[k]):
                Dict['DataType']='Datetime'
                Dict['id']=kid
                Dict['BusinessTerm']=kterm
                RulesDict['rule'] ='ValueStatistics'
                RulesDict['value'] =  [        "Min",        "Median",        "Average",        "Max"      ]                
                RulesDict['dimension'] =  ''
                RulesDict['operator']  = ''
                RulesDict['format']  = ''
                RulesDict["type"]  = "LineChart"
                rules_list.append(RulesDict)

                RulesDictout={}     
                RulesDictout['rule'] ='Outliers'
                RulesDictout['value'] =  ''              
                RulesDictout['dimension'] =  ''
                RulesDictout['operator']  = ''
                RulesDictout['format']  = ''
                RulesDictout["type"]  = "BarChart"
                rules_list.append(RulesDictout)
            if k in catFeat:
                RulesDictCat={}
                RulesDictCat['rule'] ='Frequency'
                RulesDictCat['value'] =  ''                
                RulesDictCat['dimension'] =  ''
                RulesDictCat['operator']  = ''
                RulesDictCat['format']  = '' 
                RulesDictCat["type"]  = "BarChart"
                rules_list.append(RulesDictCat)
               
            Dict['column'] =k
            Dict['rules'] = rules_list
            
           
        return Dict
        
    except Exception as e:
        return {}


def listreportsName():
    with open('reportsdb.json', 'r') as openfile:
        rjson_object = json.load(openfile)

    rdata=rjson_object
    data= rdata['reportslist']
    df = pd.DataFrame.from_dict(data)
    IDList=[]
    if not df.empty:
        IDList = df["reportName"].tolist()  
    return (IDList)

@reports.route('/api/configureReports', methods=['POST'])
def configureReports():   
    content = request.get_json()
    reportdetails= content['Reportsettings']    
    data_dict={}
    reportName=reportdetails['reportName']
    DataDomain=reportdetails['domain']
    existing_reportname_list = listreportsName()
    
    if reportName in existing_reportname_list:
        content = {}
        content['errorMsg']= 'The report name already exists'
        content['errorflag']= 'True'
        content['errorCode']= '101'
        return json.dumps(content, default=str)
    else:
        data = json.load(open("reportsdb.json","r"))        
        newrptId='RPT'+ str(int(getReportMaxId())+1)
        reportdetails['reportId']=newrptId
        reportdetails["rules"]= []
    SourceList=  data['reportslist']
    SourceList.append(reportdetails)
    data['reportslist'] = SourceList
    json.dump(data, open("reportsdb.json","w"), default=str)
    content['errorMsg']= ''
    content['errorflag']= 'False'
    content['availableCatalogues']=getcataloguefordomain(DataDomain)
    return json.dumps(content, default=str)

@reports.route('/api/configureReports', methods=['PUT'])
def EditReports():
    content = request.get_json()
    reportdetails= content['Reportsettings']    
    DataDomain=reportdetails['domain']
    data_dict={}
    reportName=reportdetails['reportName']
    reportId=reportdetails['reportId']
    with open('reportsdb.json', 'r') as openfile:
            json_object = json.load(openfile)
    eList={}
    totaleObj=[]
    data=json_object
    for obj in data['reportslist']:
        if (obj["ReportId"]==reportId):
            eList=obj
        else:
            totaleObj.append(obj)
    rdata={}

    if not eList:

        rdata['responseMsg']= 'datacatalogue not exist'
        rdata['errorflag']= 'True'
    else:
        rdata['errorMsg']= 'datacatalogue updated successfully'
        rdata['errorflag']= 'False'
        rdata['res']=reportdetails
        totaleObj.append(reportdetails)
        data['reportslist']=totaleObj
        rdata['availableCatalogues']=getcataloguefordomain(DataDomain)
        json.dump(data, open("reportsdb.json","w"))
    jsonString = json.dumps(rdata)
    return jsonString


def getReportMaxId():
        with open('reportsdb.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        dictionary ={}
        IDList=[]
        IDList.append('0')
        for obj in data['reportslist']:
                    IDList.append((obj["reportId"])[3:])
        dictionary["IdsList"] = IDList
        print(str(IDList))
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))

def RgetRuleMaxId(reportId):
        with open('reportsdb.json', 'r') as openfile:
            json_object = json.load(openfile)
        data = json_object
        IDList=[]
        IDList.append('0')
        for obj in data['reportslist']:
                if obj["reportId"]==reportId:
                   for obj1 in obj["rules"]:
                        IDList.append(obj1["rulesetId"][(len(reportId)+1):] )
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))

def isreportSourceValid(userName,userDepartment,reportDetail):
    
    createdUser=reportDetail['createdUser']    
    departmentList = reportDetail['department']
    flag = False
    flag=True

    if createdUser == userName:
        flag = True             
    if len(userDepartment) > 0 :
        for userDepart in userDepartment:
            if userDepart in departmentList:
                flag =True
                break
    
    return flag

@reports.route("/api/getList_Reports", methods=["POST"])
def getlistfromreports():
    content = request.get_json()
    userName=content['userName']
    # userCategory=content['userCategory']
    userDepartment= content['department']
    '''API to retrieve complete cleandb json log file'''
    data = json.load(open("reportsdb.json","r"))
    
    dbsingleList=[]
    for listitem in data['reportslist']:
        if isreportSourceValid(userName,userDepartment,listitem) :
            dbsingleList.append(listitem)
    data={}
    data['reportsList'] = dbsingleList
    
    jsonString = json.dumps(data, default=str)
    return jsonString
 


def RremoveAEntityDB(sourceId):
        analysisList=[]
        data={}
        with open('reportsdb.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        for obj in data['reportslist']:
            if obj["reportId"]!=sourceId:
                analysisList.append(obj)
        data['reportslist'] = analysisList
        jsonString = json.dumps(data, default=str)
        return jsonString


def RGetAEntityDB(sourceId):
    analysisList={}
    data={}
    with open('reportsdb.json', 'r') as openfile:
            json_object = json.load(openfile)

    data=json_object
    for obj in data['reportslist']:
        if obj["reportId"]==sourceId:
            analysisList=obj
            break
    data['reportslist'] = analysisList
    jsonString = json.dumps(data, default=str)
    return jsonString

@reports.route("/api/CreateRuleset_Reports", methods=["POST"])
def CreateRulesetReports():
    content = request.get_json()
    data={}
    
    reportId=content['reportId']
    newRules = content['ruleset']
    data=  json.loads(RremoveAEntityDB(reportId))
    
    AnalysisList= data['reportslist']

    rulesObject={}
    datarules= json.loads(RGetAEntityDB(reportId))
    datarulesList= datarules['reportslist']
    
 
    existingrulesList=[]
    existingrulesList=datarulesList['rules']
    rulesObject['rulesetId']=reportId + 'R' + str(int(RgetRuleMaxId(reportId))+1)
    rulesObject['selectedColumns'] = content['selectedColumns']
    rulesObject['startDate'] = content['startDate']
    rulesObject['endDate'] = content['endDate']
    #rulesObject['Referencepath'] = content['Referencepath']
    rulesObject['rulesetName']= content['rulesetName']
    rulesObject['ruleset']=newRules
    content['rulesetId']=rulesObject['rulesetId']
    existingrulesList.append(rulesObject)
    df= df_from_path('S1Titanic.xls') 
    res=getReportResults(df,newRules)
    datarulesList['rules'] =existingrulesList
    datarulesList['result']=res
    
    AnalysisList.append(datarulesList)
    data['reportslist'] = AnalysisList
    
    json.dump(data, open("reportsdb.json","w"), default=str)
    
    content['result']=res
    return json.dumps(content, default=str)



def editRulesetreport(content):
    data={}
    
    sourceId=content['reportId']
    newRules = content['ruleset']
    rulesetId= content['rulesetId']

    data=  json.loads(RremoveAEntityDB(sourceId))
    AnalysisList= data['reportslist']

    rulesObject={}
    datarules= json.loads(RGetAEntityDB(sourceId))
    datarulesList= datarules['reportslist']
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
    data['reportslist'] = AnalysisList
    json.dump(data, open("reportsdb.json","w"), default=str)
    return json.dumps(content, default=str)

def getcataloguefordomain(DataDomain):    
        resdata={}
        with open('dCatalogue.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        dictionary ={}
        IDList=[]
        populated =  [{'Column':x['Column'],'BusinessTerm':x['BusinessTerm'] ,  'DataDomain':x['DataDomain'] } for x in data['datacatalogues'] if x['DataDomain'] in DataDomain]
        print(populated)
        return(populated)


def getcatalogueforterms(terms):   

        domainList=[x['domain'] for x in terms] 
        termList=[x['term'] for x in terms] 
        resdata={}
        with open('dCatalogue.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        dictionary ={}
        IDList=[]
        populated =  [{'Column':x['Column'] ,'id':x['id'],'BusinessTerm':x['BusinessTerm'] } for x in data['datacatalogues'] if (x['BusinessTerm'] in termList and x['DataDomain'] in domainList)]
        print(populated)
        return(populated)

def outlierDetection_reports(columns, data):
    outliers = []
    outlierPercentDict = {}
    for column in columns:
        upper_limit = data[column].mean() + data[column].std() * 3
        lower_limit = data[column].mean() - data[column].std() * 3
        outliers = data[(data[column] > upper_limit) | (data[column] < lower_limit)][column].to_list()
        
        total_count = data[column].count()
        outlier_cnt = len(outliers)
        outlierPercent = (outlier_cnt/total_count) * 100
        outlierPercentDict["outliers"] = outlierPercent
        normalData_cnt = total_count - outlier_cnt
        normalDataPercent = (normalData_cnt/total_count) * 100
        outlierPercentDict["normal"] = normalDataPercent
    return outliers, outlierPercentDict

def getReportResults(df,ruleset):
    resultDictList=[]
    resultstobesharedList=[]
    for r in ruleset:
                frametbpcol= df.copy(deep=True)

                col=r['column']
                term=r['BusinessTerm']
                dtype=r["DataType"]
                row_idx = frametbpcol[frametbpcol[col].isnull()].index.to_list()
                frametbpcol.drop(row_idx, axis = 0, inplace = True)
                frametbpcolnew= frametbpcol.copy(deep=True)
                framenew= frametbpcolnew.copy(deep=True)
                resultstobeshared={}
                resultstobeshared['column']=col
                resultstobeshared['BusinessTerm']=term
                for ru in r['rules']:
                    if ru['rule'] =='ValueStatistics':
                        oper= ru['value']
                        valuestatistics={}
                        framecopy1= framenew.copy(deep=True)
                        if 'Average' in oper:
                                resultDict={}
                                framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                res=framenew [col].mean()
                                resultDict["Dimension"]="Average of "+ term
                                resultDict["value"]=str(round(res,2))

                                valuestatistics["AverageValue"]=str(round(res,2))
                                resultDictList.append(resultDict)
                        if 'Mean' in oper:
                                resultDict={}
                                framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                res=framenew [col].mean()
                                resultDict["Dimension"]="Mean of "+ term
                                resultDict["value"]=str(round(res,2))
                                valuestatistics["MeanValue"]=str(round(res,2))
                                resultDictList.append(resultDict)
                        if 'Median' in oper:
                                resultDict={}
                                framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                res=framenew[col].median()
                                resultDict["Dimension"]="Median of "+ term
                                resultDict["value"]=str(round(res,2))
                                valuestatistics["MedianValue"]=str(round(res,2))
                                resultDictList.append(resultDict)
                        if 'Min' in oper:
                                resultDict={}
                                framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                res=framenew[col].min()
                                resultDict["Dimension"]="Minimum of "+ term
                                resultDict["value"]=str(round(res,2))
                                valuestatistics["MinimumValue"]=str(round(res,2))
                                valuestatistics["MinValLength"]=len(str(res))
                                resultDictList.append(resultDict)
                        if 'Max' in oper:
                                resultDict={}
                                framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                res=framenew[col].max()
                                resultDict["Dimension"]="Mimimum of "+ term
                                resultDict["value"]=str(round(res,2))
                                valuestatistics["MaximumValue"]=str(round(res,2))
                                valuestatistics["MaxValLength"]=len(str(res))
                                resultDictList.append(resultDict)
                        resultstobeshared['ValueStatistics']=valuestatistics    
                    if ru['rule'] =='LengthStatistics':
                        oper= ru['value']
                        framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                        #framenew = framenew.replace(np.nan, 0, regex=True)
                        columns1 = framenew.astype(str)
                        columnsSeries = pd.Series(columns1.values.flatten())
                        
                        
                        valuestatistics1={}
                        framecopy1= framenew.copy(deep=True)
                        if 'Average' in oper:
                                resultDict={}
                                
                                res=round(columnsSeries.str.len().mean(), 2)
                                resultDict["Dimension"]="Average of "+ term
                                resultDict["value"]=str(round(res,2))

                                valuestatistics1["Average"]=str(round(res,2))
                                resultDictList.append(resultDict)
                        if 'Median' in oper:
                                resultDict={}
                                
                                res=columnsSeries.str.len().median()
                                resultDict["Dimension"]="Median of "+ term
                                resultDict["value"]=str(round(res,2))
                                valuestatistics1["Median"]=str(round(res,2))
                                resultDictList.append(resultDict)
                        if 'Min' in oper:
                                resultDict={}
                                res=columnsSeries.str.len().min()
                                resultDict["Dimension"]="Minimum of "+ term
                                resultDict["value"]=str(round(res,2))
                                valuestatistics1["Min"]=str(round(res,2))
                                
                                resultDictList.append(resultDict)
                        if 'Max' in oper:
                                resultDict={}
                                res=columnsSeries.str.len().max()
                                resultDict["Dimension"]="Mimimum of "+ term
                                resultDict["value"]=str(round(res,2))
                                valuestatistics1["Max"]=str(round(res,2))
                                resultDictList.append(resultDict)
                        resultstobeshared['LengthStatistics']=valuestatistics1        
                    if ru['rule']=='Frequency':
                        frquencydict={}
                        oper= 'Frequency'                        
                        framecopy1= framenew.copy(deep=True)
                        if oper=='Frequency':    
                            resultDict={}                        
                            value_counts = framecopy1[col].value_counts().head(15)
                            resDf = value_counts.where(pd.notnull(value_counts), 'None')
                            df_value_counts = pd.DataFrame(value_counts)
                            n= len(value_counts)
                            df_value_counts = df_value_counts.reset_index()
                            df_value_counts.columns = ['unique_values', 'counts'] # change column names
                            freqencyDict= df_value_counts.to_dict('records')
                            
                            #data_dict = resDf.to_dict('index')
                            resultDict["Dimension"]="Total count of Categories in "+ term
                            resultDict["value"]=resDf
                            resultDictList.append(resultDict)
                        resultstobeshared['frequncyAnalysis'] =freqencyDict
                    if ru['rule']=='Outliers' :
                        outlierList=[]
                        outlierPercentDict={}
                        framecopy2= framenew.copy(deep=True)
                        if dtype=="Numeric":
                            columnslist=[col]
                            outlierList, outlierPercentDict = outlierDetection_reports(columnslist,  framecopy2)
                        resultstobeshared['outliersList'] = outlierList
                        resultstobeshared['outliersPercent'] = outlierPercentDict
      
                resultstobesharedList.append(resultstobeshared)    
    return resultstobesharedList
        