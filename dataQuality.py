

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



def columnwiseCompletenesscheck(frame):
    percent_missing = round(100- (frame.isnull().sum() * 100 / len(frame)),2)
    percent_missing_count =(frame.isnull().sum())
    missing_value_df = pd.DataFrame({'column_name': frame.columns,
                                 'percent_missing': percent_missing,
                                 'count':percent_missing_count})  
    return (missing_value_df)                                

def columnwiseuniquenesscheck(frame,col):
    DNull={}
    dupPerc= 100 -((frame.duplicated([col]).mean())*100)
    outliercount=str(frame.duplicated([col]).sum())
    DNull['ruleMismatchcount'] =str(outliercount)
    DNull['result']=dupPerc
                            
    return DNull

def columnwiseDataTypecheck(frame,col,dataType):
    DNull={}
    DNull['ruleType']='DataType'
    DNull['rulevalue']='alphabets'
    DNull['ruleMismatchcount'] ='0'
    DNull['result']=100
    if dataType !='':
            percen=100
            
            if dataType=='alphabets' or dataType=='Alphanumeric' or dataType=='Text' :
                            cntalnum=frame[col].str.isalnum().sum()
                            cnttotal=frame[col].count()
                            percen=((cntalnum)/(cnttotal))*100
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            DNull['result']=percen
                            
            elif dataType=='Numeric':
                            frame[col]=frame[col].astype(str)
                            frame[col]=frame[col].str.replace(".", "",regex=False)
                            cntalnum=frame[col].str.isdigit().sum()
                            cnttotal=frame[col].count()
                            percen=((cntalnum)/(cnttotal))*100
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            DNull['result']=percen

            return DNull               


def columnwiseLengthCheck(frame,col,Length):
    percen=100
    DNull={}
    if Length!='':
        if (int(float(Length))==frame[col].astype(str).str.len().min()) and  (int(float(Length))==frame[col].astype(str).str.len().max()):
            percen=100
            DNull['ruleType']='Length'
            DNull['rulevalue']=Length
            DNull['ruleMismatchcount'] =str(0)
            DNull['result']=percen    
        else:
            percen=(100-((((frame[col].astype(str).str.len() !=int(float(Length))).sum())/(len(frame[col])))*100) )
            cnttotal=len(frame[col])
            cntalnum=((frame[col].astype(str).str.len() !=int(float(Length))).sum())
            DNull['ruleType']='Length'
            DNull['rulevalue']=Length
            DNull['ruleMismatchcount'] =str(cntalnum)
            DNull['result']=percen    
    return DNull    

def columnwiseMinLengthCheck(frame,col,Length):
    percen=100
    DNull={}
    if Length!='':
            percen=(100-((((frame[col].astype(str).str.len().min() !=int(float(Length))).sum())/(len(frame[col])))*100) )
            cntalnum=((frame[col].astype(str).str.len().min() !=int(float(Length))).sum())
            DNull['ruleType']='Length'
            DNull['rulevalue']=Length
            DNull['ruleMismatchcount'] =str(cntalnum)
            DNull['result']=percen 
    return DNull

def columnwiseMAXLengthCheck(frame,col,Length):
    percen=100
    DNull={}
    if Length!='':
            percen=(100-((((frame[col].astype(str).str.len().max() !=int(float(Length))).sum())/(len(frame[col])))*100) )
            cntalnum=((frame[col].astype(str).str.len().max() !=int(float(Length))).sum())
            DNull['ruleType']='Length'
            DNull['rulevalue']=Length
            DNull['ruleMismatchcount'] =str(cntalnum)
            DNull['result']=percen 
    return DNull     

def columnwiseValueCheck(frame,col,value):
            returnperc=100
            if not frame.empty:
                        ru=value
                        ComValue= ru['value']

                        oper=ru['operator']
                        isAccuracy=True
                        framecopy1= framenew.copy(deep=True)
                        if oper=='equalto' or oper=='Shouldbe':

                            if(ComValue.isnumeric()):
                                framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                elnum_rows = len(framenew[framenew[col].astype('float')==float(ComValue)])
                                
                            else:
                               
                                elnum_rows = len(framenew[str(framenew[col])==(ComValue)])
                                
                        elif oper=='greaterthanequalto':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')>=float(ComValue)])
                            
                        elif oper=='lessthanequalto':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')<=float(ComValue)])
                            
                        elif oper=='greaterthan':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')>float(ComValue)])
                            
                        elif oper=='lessthan':
                            framenew [col] = pd.to_numeric(framenew[col], errors='coerce')
                            framenew = framenew.replace(np.nan, 0, regex=True)
                            elnum_rows = len(framenew[framenew[col].astype('float')<float(ComValue)])
                            
                        refCount1=elnum_rows

                        reftotalCount1=len(framecopy1[col])
                        refPerc1= (refCount1 / reftotalCount1)*100
                        returnperc= (refPerc1)
            return returnperc

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
                        
                        querystring= querystring + condn["value"]
                    else:
                        
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
                            
                            querystring= querystring + listGroup["value"]
                        else:
                            
                            querystring= querystring + "'"+listGroup["value"] + "'"
                    querystring=querystring + ')'                
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
                       
                        querystring= querystring + condn["value"]
                    else:
                        
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


def columnwiseFormulacheck(framenew,col,formulaval):
    ru=formulaval
    retperc=100

    if ru['rule'] =='Formula':
                        rulename=""
                        isAccuracy=True
                        formula= ru['value']
                        ifFlag='false'
                        formulaText=""
                        for valcol in formula:
                            formulaText=valcol['formulaText'] 
                            if valcol['logic']=="If":
                                ifFlag='true'                                
                                break
                        if ifFlag=='true':
                            #framenew= framenew.dropna()

                            formula= ru['value']
                            querystring =GetQueryStringifnew(formula)
                            invertedQString=GetinvertQueryifString(formula)
                            framecopy= framenew.copy(deep=True)
                            

                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                                
                                idf2= framenew.query(invertedQString)
                                x = pd.concat([df2, idf2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                
                                num_rows= len(framecopy[col])-len(r7)
                                                              
                            rulename=querystring
                        else:
                            querystring= formulaText#ru['formulaText']
                            formula= ru['value']
                            
                            invertedQString=querystring
                            framecopy= framenew.copy(deep=True)
                            
                            if querystring!="":
                                framenew = framenew.replace(np.nan, 0, regex=True)
                                #framenew=getFormatteddf(formula,framenew)
                                
                                df2=framenew.query(querystring)
                                num_rows=len(df2)
                               
                                
                                x = pd.concat([framenew, df2])
                                r7= x.drop_duplicates(keep=False, inplace=False)
                                
                                num_rows= len(framenew[col])-len(r7)
                               
                            rulename=querystring

                           
                        refCount=0
                        reftotalCount=0
                        refPerc=0
                        refCount=num_rows
                        #r7.index = r7.index + 2
                        reftotalCount=len(framecopy[col])
                        if reftotalCount!=0:
                            refPerc= (refCount / reftotalCount)*100
                            retperc=(refPerc)
    return retperc                    

                                
def checkQuality(df,columns):
    
    rulesObject={}
    resultsetDictlist=[]
   
    completeness=[]
    Accuracy=[]
    Uniqueness=[]
    Integrity=[]
    Validity=[]
    count=0
    
    with open('tempdb.json', 'r') as openfile:
                json_object = json.load(openfile)

    data = json_object
    return data['dq']   


def checkDataQuality(df,selectedColumns):
    dictQualityList=[]
    completenessRes=columnwiseCompletenesscheck(df)
   
    overallperc=[]
    for k in selectedColumns:
        dictQulityResult={}
        dictQulityResult['ColumnName']=k
        dictQuality={}
        
        # dictQualityUniqueness={}
        # resultdup= columnwiseuniquenesscheck(df,k)
        # outlierCount=resultdup['ruleMismatchcount'] 
        # dictQualityUniqueness['value']=str(round(resultdup['result'],2))
        # dictQualityUniqueness['info']=[{ "rule": "Duplicate count check","OutlierCount": outlierCount}]
        # overallperc.append(round(resultdup['result'],2))
        
        

        # dictQuality['Uniqueness']=dictQualityUniqueness
        query="column_name =='"+ k+"'"
        
        sub_df=(completenessRes.query(query))    
        dictQualityCompleteness={}
        dictQualityCompleteness['value']=str(round(sub_df.iloc[0]['percent_missing'], 2))
        ouyliercnt=sub_df.iloc[0]['count']
        dictQualityCompleteness['info']=[{ "rule": "Null check","OutlierCount": str(ouyliercnt)}]
        dictQuality['Completeness']=dictQualityCompleteness
        overallperc.append(round(sub_df.iloc[0]['percent_missing'], 2))
        rule= getdefaultrules(df,k)
        ruleinfo=[]
        ruleinfodict={}
        ruleinfodict1={}
        validityper=[]
        ruleforValidity=[x for x in rule['rules'] if x['rule'] =='DataType']
        
        length=0
        datatype=''
        percenValidityfordatatype=100
        if ruleforValidity:
                
                for rulee in ruleforValidity:
                   
                    datatype=rulee['value']
                    DNull=columnwiseDataTypecheck(df,k,datatype)                    
                    outlierCount=DNull['ruleMismatchcount'] 
                    percenValidityfordatatype=  DNull['result']
                    ruleinfodict  ={ "rule": "DataType match check (" + DNull['ruleType'] + " : "+DNull['rulevalue']+") ",
                    "OutlierCount": outlierCount}
                    ruleinfo.append(ruleinfodict)
                    validityper.append(percenValidityfordatatype)

        ruleforValidityMinLength=[x for x in rule['rules'] if x['rule'] =='MinLength']
        #print(ruleforValidityMinLength)   
        ruleforValidityMaxLength=[x for x in rule['rules'] if x['rule'] =='MaxLength']
        #print(ruleforValidityMaxLength)  
        ruleforValidityLength=[x for x in rule['rules'] if x['rule'] =='Length']
        #print(ruleforValidityLength)     
        percenValidity=100

        if  ruleforValidityMinLength or  ruleforValidityMaxLength or  ruleforValidityLength:
            if  ruleforValidityMinLength:
                for rulee in ruleforValidityMinLength:
                    length=rulee['value']
                    
                          
                    dresult=columnwiseMinLengthCheck(df,k,length)

                    outlierCount=dresult['ruleMismatchcount'] 
                    percenValidity=  dresult['result']
                    ruleinfodict  ={ "rule": "Length check (Min Length should be " + dresult['rulevalue']+") ",
                    "OutlierCount": outlierCount}
                    ruleinfo.append(ruleinfodict)  
                    validityper.append(percenValidity)
            if  ruleforValidityMaxLength:
                for rulee in ruleforValidityMaxLength:
                    length=rulee['value']      
                    dresult=columnwiseMAXLengthCheck(df,k,length)
                    outlierCount=dresult['ruleMismatchcount'] 
                    percenValidity=  dresult['result']
                    ruleinfodict  ={ "rule": "Length check (Max Length should be " + dresult['rulevalue']+") ",
                    "OutlierCount": outlierCount}
                    ruleinfo.append(ruleinfodict) 
                    validityper.append(percenValidity)  
            if  ruleforValidityLength:
                for rulee in ruleforValidityLength:
                    length=rulee['value']
                    dresult=columnwiseLengthCheck(df,k,length)
                    outlierCount=dresult['ruleMismatchcount'] 
                    percenValidity=  dresult['result']
                    ruleinfodict  ={ "rule": "Length check (Max Length should be " + dresult['rulevalue']+") ",
                    "OutlierCount": outlierCount}
                    ruleinfo.append(ruleinfodict) 
                    validityper.append(percenValidity)
           
            #print(validityper)
        if len(validityper)>0:
            validitypercenatge=  sum(validityper)/len(validityper)
        else:
            validitypercenatge=100    
        dictQualityvalidity={}
        dictQualityvalidity['value']=str(round(validitypercenatge, 2))
        dictQualityvalidity['info']= ruleinfo      
        overallperc.append(round(validitypercenatge, 2))
        dictQuality['Validity']=dictQualityvalidity    

        dictQualityConsistency={}
        dictQualityConsistency['value']=str(round(100.00, 2))
        dictQualityConsistency['info']= [{ "rule": "No Rules Executed",
        "OutlierCount": '0'}]      

        dictQuality['Consistenecy']=dictQualityConsistency 
        overallperc.append(round(100.00, 2))
        dictQualitytimliness={}
        dictQualitytimliness['value']=str(round(100.00, 2))
        dictQualitytimliness['info']= [{ "rule": "file reception frequency check","OutlierCount": '0'}]      
        overallperc.append(round(100.00, 2))
        dictQuality['Timeliness']=dictQualitytimliness 


        ruleforAccuracy=[x for x in rule['rules'] if x['rule'] =='Formula']
       
        length=0
        datatype=''
        ruleinfoaccuracy=[]
        percenAccuracyfordatatype=100
        dictQualityAccuracy={}
        dictQualityAccuracy['value']=str(round(100.00, 2))
        dictQualityAccuracy['info']= [{ "rule": "No Rules executed","OutlierCount": '0'}]
        if not ruleforAccuracy:
                ruleinfodict2  ={}

                for rulee in ruleforAccuracy:
                    datatype=ruleforAccuracy['value']
                    ruleinfoaccuracy=[{ "rule": "Formula check"+datatype,"OutlierCount": '0'}]
                    percenAccuracyfordatatype=columnwiseFormulacheck(df,k,datatype)  
                
                    dictQualityAccuracy['value']=str(round(percenAccuracyfordatatype, 2))
                    dictQualityAccuracy['info']= ruleinfoaccuracy      

        dictQuality['Accuracy']=dictQualityAccuracy       
        overallperc.append(round(100.00, 2))     
                
        dictQulityResult['detail']=dictQuality
        if len(overallperc)>0:
            resoverall=sum(overallperc)/len(overallperc)
            dictQulityResult['overall']=str(round(resoverall,2))
        dictQualityList.append(dictQulityResult)

    
    #print(dictQualityList)    
    return dictQualityList

def savequlaitychecktodb(results,sourcepath):

    outputdata = json.load(open("qualityResults.json","r"))
    OAnalysisList=  outputdata['Results']
    outputResData={}
    LDBDataList=[d for d in OAnalysisList if (d["sourcepath"])==sourcepath]
    if len(LDBDataList)>0:
        return 'true'
    else:    
        content={}
        
        content['sourcepath']= sourcepath
        content['results']=results
        OAnalysisList.append(content)
        outputdata['Results'] = OAnalysisList
        
        json.dump(outputdata, open("qualityResults.json","w"), default=str)
        return 'true'


def count_digits(string):
    return sum(item.isdigit() for item in string)

def getdefaultrules(df,selectedColumn):   
    df= df.head(3000)
    rules=[]
    Dict = {}
    try:
        if selectedColumn!="":
            k=selectedColumn
            statisticAnal= []

            corrMatrix={}
            RulesDict = {}
            rules_list=[]
            if is_string_dtype(df[k]):
                RulesDict['rule'] ='DataType'
                RulesDict['value'] =  'Text'
                RulesDict['dimension'] =  'Validity'
                RulesDict['operator']  = 'Shouldbe'

                rules_list.append(RulesDict)
                if (1==1):
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
                
                
               
            if is_datetime64_any_dtype(df[k]):
                RulesDict['rule'] ='DataType'
                Dict['DataType']='Datetime'
                RulesDict['value'] =  'Datetime'
                
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
                

            
            if (df[k].nunique()!=0):
                Dict['column'] =k
                Dict['rules'] = rules_list
                rules.append(Dict)
            else:
                Dict['column'] =k
                Dict['rules'] = rules_list
                rules.append(Dict)
        return Dict
       
    except Exception as e:
        print(str(e))
     
