import pandas as pd
import json
import os
import os.path
from io import StringIO
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_datetime64_any_dtype
from werkzeug.utils import secure_filename
from CorrelationFunctions import compute_associations
from datetime import date
import time
import datetime

from Datacatlog import getcatalogue,editDatacatalogue,createDatacatalogue,getcatalogueforcolumns
import re
import numpy as np
import csv


from sqlalchemy import create_engine, false, true
import sqlite3

def find_delimiter(filename):
    print("filename=",filename)
    sniffer = csv.Sniffer()
    with open(filename) as fp:
        delm = sniffer.sniff(fp.read(5000)).delimiter
        print("delimiter",delm)
        if(delm ==','):
            print("in if condition")
            Err_Msg="True"
        else :
            print("csv error")
            Err_Msg="False"
    return Err_Msg

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

def getPC_SourceMaxId(sourceCategory,db):
    if db == "profile":
        with open('profiledb.json', 'r') as openfile:
            json_object = json.load(openfile)
    elif db == "clean":
        with open('cleandb.json', 'r') as openfile:
            json_object = json.load(openfile)
    data = json_object
    IDList=[]
    IDList.append(0)
    for obj in data['SourceDetailsList']:
        if obj["sourceCategory"] == sourceCategory:
            # id_nr = obj["sourceId"][5:]
            if len(obj["sourceId"]) > 3:
                id_nr = obj["sourceId"][5:]
            else:
                id_nr = obj["sourceId"][1:]
            print("ID_nr:",id_nr)
            IDList.append(int(id_nr)) ## Get ID nr in the end
    print('Source ID nr list for SourceCategory:{} in {}dB:'.format(sourceCategory,db),IDList)
    print("Max ID nr for SourceCategory - {} in {} dB:".format(sourceCategory,db),max(IDList))
    return max(IDList)

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

def addCleaningSourceFromProfile(sourcedb,sourcedetails):
    data = json.load(open("cleandb.json","r"))
    SourceList=  data['SourceDetailsList']
    sourcedetails["source"]=sourcedb
    sourceDataName=sourcedetails['sourceDataName']
    sourceCategory = sourcedetails['sourceCategory']
    db="clean"
    newSourceId = sourceDataName[0] + sourceDataName[-1] + '_CS'+ str(int(getPC_SourceMaxId(sourceCategory, db)+1))
    sourcedetails['sourceId']= newSourceId
    SourceList.append(sourcedetails)
    data['SourceDetailsList'] = SourceList
    json.dump(data, open("cleandb.json","w"), default=str)
    return "true"

def updatecommondb(sourcedb,sourcedetails):    
    with open('commondb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

    rdata=rjson_object
    data= rdata['commonSourceList']
    df = pd.DataFrame.from_dict(data)
    df2=sourcedetails
    df = df.append(df2, ignore_index = True)
    rdata={}

    if not df.empty:
        rdata['commonSourceList']=[]
        rdata['commonSourceList']=df.to_dict('records')
        json.dump(rdata, open("commondb.json","w"))
        rdata['responseMsg']= 'Department added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'Department already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
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

def isprofileSourceValid(userName,userCategory,userDepartment,sourceDetail):
    source = sourceDetail   
    print("source=",source)   
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

def df_from_path_profile(sourcepath):
    '''Returns a pandas dataframe when given a source path'''
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    sqllitePath='sqlite:///'+ os.path.splitext(os.path.basename(sourcepath))[0]+".db"
    print('qlite :' + sqllitePath )
    fileize= os.path.getsize(sourcepath)
    filesize = fileize/(1024*1024)
    if filesize>12:
        print("in large")
        df=getdffromsql(sqllitePath)
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
                # df = pd.read_json(sourcepath, orient = "index")
               
        elif(file_ext==".xml"):
            try:
                df = pd.read_xml(sourcepath)
            except:
                err_msg = {"Error":"XML file read error. XML file has faulty structure which mismatches the reference example or\
                            XML file may contain column names with whitespaces or special characters other than '_' "}
                return err_msg
            print(df)    
    df = force_datetime_conversion(df) ##Force DataType conversion to suitable date/time cols
   
    return df

def GetCorrelationMatrix(df,cols_data_type = 'mixed', method="None"):
    #df = df_from_path(sourcepath)
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

def sorted_corr_matrix_per_col(cm_df,col):
  '''Returns the top 10 correlated pairs per column when a correlation matrix is given as input'''
  correlation_mat = cm_df.copy()
  nr_rank_elements = 10
  np.fill_diagonal(correlation_mat.values, np.nan)
  corr_values = correlation_mat.unstack()
  sorted_pairs = corr_values.sort_values(kind="quicksort", ascending = False)
  sorted_pairs_df = pd.DataFrame(sorted_pairs, columns = ['Corr_Coeff'])
  print('test')
  positive_pairs = sorted_pairs[sorted_pairs >= 0]
  negative_pairs = sorted_pairs[sorted_pairs < 0]
  positive_pairs_df = pd.DataFrame(positive_pairs, columns = ['Corr_Coeff'])
  negative_pairs_df = pd.DataFrame(negative_pairs, columns = ['Corr_Coeff'])
  print('positive:')

  try:
    positive_pairs_dict,positive_pairs_list = Ordered_dict(positive_pairs_df.loc[col][:nr_rank_elements].dropna(axis = 0, how = 'any').rename_axis(col))
  except:
    positive_pairs_dict={}
    positive_pairs_list = []
  if len(negative_pairs) == 0 or col not in (negative_pairs_df):
        if check_dict_same_values(positive_pairs_dict) :
            print('inside loop')
            dictt={}
            dictt['positiveSummary'] = []
            dictt['negativeSummary'] = []
            return dictt
        dictt={}
        dictt['positiveSummary'] =positive_pairs_list
        dictt['negativeSummary'] = []
        return dictt
  else:
        negative_pairs_dict,negative_pairs_list = Ordered_dict(negative_pairs_df.loc[col][:nr_rank_elements].dropna(axis = 0, how = 'any').rename_axis(col).sort_values(by = ['Corr_Coeff'],ascending = True))

        dictt={}
        dictt['positiveSummary'] =positive_pairs_list
        dictt['negativeSummary'] =negative_pairs_list
        return dictt

def check_dict_same_values(x):
    list_object = list(x.values())
    return all(i == list_object [0] for i in list_object )

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

def addProfileSourceFromcleaning(sourcedb,sourcedetails):
    data = json.load(open("profiledb.json","r"))
    SourceList=  data['SourceDetailsList']
    sourcedetails["source"]=sourcedb
    sourceDataName=sourcedetails['sourceDataName']
    sourceCategory = sourcedetails['sourceCategory']
    db="profile"
    newSourceId = sourceDataName[0] + sourceDataName[-1] + '_PS'+ str(int(getPC_SourceMaxId(sourceCategory, db)+1))
    sourcedetails['sourceId']= newSourceId
    SourceList.append(sourcedetails)
    data['SourceDetailsList'] = SourceList
    json.dump(data, open("profiledb.json","w"), default=str)
    return "true"

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

def saveResultsets(data,name):
    #jsonString = json.dumps(data)
    filename= name + '.json'

    json.dump(data, open(filename,"w"), default=str)
    return True

def df_to_json(df):
    resDf = df.where(pd.notnull(df), 'None')
    data_dict = resDf.to_dict('index')

    content={}
    content['Preview'] = data_dict
    jsonString = json.dumps(content, default=str)
    return jsonString

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


def getdffromsql(dbName):
    
    csv_database = create_engine(dbName)
    df = pd.read_sql_query('SELECT * FROM tblSource limit 3000000', csv_database)  
    df.drop('index', axis=1, inplace=True)  
    return df


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
    columnsnew=columns.copy()
    columnsnew=columnsnew.fillna(0)
    conOutDF[conHead[7]] = round(columnsnew.std(), 2)
   
    columns1 = columns.astype(str)
    columnsSeries = pd.Series(columns1.values.flatten())
    
    conOutDF[conHead[8]] = columnsSeries.str.len().min()
    conOutDF[conHead[9]] = columnsSeries.str.len().max()
    conOutDF[conHead[10]] = round(columnsSeries.str.len().mean(), 2)
    conOutDF[conHead[11]] = columnsSeries.str.len().median()

    
    return  conOutDF.to_dict('records')#.to_json(orient='records')[1:-1].replace('},{', '} {')

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

def Ordered_dict(df):
  keys = list(df.index.values)
  values = list(df.values.flatten())

  dict_object = {}
  newDictList=[]
  for k, v in zip(keys, values):
    newDict={}
    newDict['column']=k
    newDict['value']=round(v,2)
    newDictList.append(newDict)
    dict_object[k] = v
  return dict_object,newDictList

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

def conditions_chainer(conditions):
  """Helper function to chain dataframe query conditions
  from a conditions dictionary and return them as a single string"""
  query_condition = ''
  for cond,i in zip(conditions.values(),range(1,len(conditions)+1)):
      cond = cond.replace('\'', '"')
      print("Cond{}:".format(i),cond)
      if i == len(conditions):
        # print("Last condition")
          query_condition += cond
      else:
          query_condition += cond + " and "
  return query_condition

def getcleanMaxId(cleanedFileLogs):       
    IDList=[]
    IDList.append('0')
    for obj in cleanedFileLogs:               
        IDList.append(obj["cleanSourceId"][(2):] )
    dli2 = [int(s) for s in IDList]
    return (str(max(dli2)+1))

def getSourceRules(sourceId):
    analysisList=[]
    data={}
    with open('db.json', 'r') as openfile:
        json_object = json.load(openfile)
    data = json_object
    temprules=[]
    LDBDataList=[]
    LDBDataList=[d for d in data['Analysis'] if (d["sourceId"])==sourceId]
    
    for obj in LDBDataList:
       
        tempDict={}
        uploadSourceList=[]
        cleanedhistList = []
        recentsourceUpload={}
        tempDict['sourceId']= obj['sourceId']
        sourceId= obj['sourceId']
        tempDict['source']= obj['source']
        sourceData= obj['source']
        tempDict['reference']= obj['reference']
        tempDict['settings']=obj['settings']
        

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

            startDate_obj =  datetime.datetime.strptime(obj1['startDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
            endDate_obj = datetime.datetime.strptime(obj1['endDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

            if startDate_obj.date() <= date.today() <= endDate_obj.date():
                temprulesDict["Rulestatus"]= "Active"
            elif startDate_obj.date() >= date.today():
                temprulesDict["Rulestatus"]= "Inactive"
            else:
                temprulesDict["Rulestatus"]= 'Expired'

            temprules.append(temprulesDict)
    tempResult={}
    tempResult['rules']= temprules
    return temprules


def numeric_col_relational_query(df, col, formula ):
    operator1 = formula["operator1"]
    cond_value1 =  formula["cond_value1"]
    operator2 = formula["operator2"]
    cond_value2 =  formula["cond_value2"]
    if operator2 == "" and cond_value2 == "":
        if operator1 == "=":
            df_result = df[df[col] == cond_value1]
        if operator1 == "<":
              df_result = df[df[col] < cond_value1]
        if operator1 == ">":
          df_result = df[df[col] > cond_value1]
        if operator1 == "<=":
          df_result = df[df[col] <= cond_value1]
        if operator1 == ">=":
          df_result = df[df[col] >= cond_value1]
    else:
        df_result = df[df[col].between(cond_value1, cond_value2)]
    return df_result

def Top_three_percentage(df):
    selectedColumns = list(df.columns.values)
    mydict = {}
    mydictlist = []
    for i in selectedColumns:
        mydict = {}
        if 'datetime' in str(df[i].dtype):
            top_3 = df[i].dt.strftime("%d-%m-%Y").value_counts().nlargest(3)
        else:
            top_3 = df[i].value_counts().nlargest(3)
        top_3_per = round((top_3/len(df))*100,4)        
        mydict.update(top_3_per)
        mydictlist.append(mydict)
    top = dict(zip(selectedColumns, mydictlist))
    return top

def gdpr(sourcepath,seeMoreEnabled):	
    # content = request.get_json()	
    # sourcepath= content['sourcepath']	
    if(seeMoreEnabled=="YES"):	
        df = df_from_path(sourcepath)	
    else:	
        df =limitedDf_from_path(sourcepath,1000) 
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    address_pattern = re.compile(r'\b(?:\d+\s+\w+\s\w+|\w+\s+\w+),?\s*(?:[A-Z]{2}|U\.S\.A\.|\d{6})\b', re.IGNORECASE)
    age_pattern = r'\b\d{1,3}\b'
    phone_pattern = re.compile(r'\+?\d{0,3}\s?\d{3}[-\s]?\d{3}[-\s]?\d{4}')
    name_pattern = re.compile(r'\b(?:Mr\.|Ms\.|Miss\.|Prof\.|Dr\.|[A-Z][a-z]*)?\s*[A-Z][a-z]+\b', re.IGNORECASE)
    number_pattern= r'(\d+(?:\.\d+)?(?:,\s*\d+(?:\.\d+)?)*?)'
    gender_keywords = ['male', 'female', 'lesbian', 'gay', 'transgender', 'non-binary']
# iterate over the columns
    for col in df.columns:
    # check if the column contains text data
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
        # use str.contains method to find gender keywords
#         contains_gender = df[col].str.contains('|'.join(gender_keywords), case=False)
            contains_gender = df[col].fillna(False).str.contains('|'.join(gender_keywords), case=False)
        # mask gender keywords with asterisks
#         df.loc[contains_gender, col] = df[contains_gender][col].str.replace('|'.join(gender_keywords), '***', case=False)
            df.loc[contains_gender.fillna(False), col] = df[contains_gender.fillna(False)][col].fillna('').str.replace('|'.join(gender_keywords), '***', case=False)

# find columns containing email addresses, addresses, age, and phone numbers
    email_cols = []
    address_cols = []
    age_cols = []
    phone_cols = []
    name_cols=[]
    number_cols=[]
    for col in df.columns:
        if df[col].apply(lambda x: re.search(email_pattern, str(x))if pd.notnull(x) else False).any():
            email_cols.append(col)
        if df[col].apply(lambda x: re.search(address_pattern, str(x))if pd.notnull(x) else False).any():
            address_cols.append(col)
        if df[col].apply(lambda x: re.search(age_pattern, str(x))if pd.notnull(x) else False).any():
            age_cols.append(col)
        if df[col].apply(lambda x: re.search(phone_pattern, str(x))if pd.notnull(x) else False).any():
            phone_cols.append(col)
        if df[col].apply(lambda x: re.search(name_pattern, str(x))if pd.notnull(x) else False).any():
            name_cols.append(col)
        if df[col].apply(lambda x: re.search(number_pattern, str(x))if pd.notnull(x) else False).any():
            number_cols.append(col)

# mask the email, address, age, and phone number columns by replacing with '***'
    for col in email_cols + address_cols + age_cols + phone_cols+name_cols:
        df[col] = df[col].apply(lambda x: re.sub(email_pattern, '***', str(x))if pd.notnull(x) else x)
        df[col] = df[col].apply(lambda x: re.sub(address_pattern, '***', str(x))if pd.notnull(x) else x)
        df[col] = df[col].apply(lambda x: re.sub(age_pattern, '***', str(x))if pd.notnull(x) else x)
        df[col] = df[col].apply(lambda x: re.sub(phone_pattern, '***', str(x))if pd.notnull(x) else x)
        df[col] = df[col].apply(lambda x: re.sub(name_pattern, '***', str(x))if pd.notnull(x) else x)
        df[col] = df[col].apply(lambda x: re.sub(number_pattern, '***', str(x))if pd.notnull(x) else x)
# save the masked dataframe to a CSV file
    df.to_csv('masked_data.csv', index=False)
    # json_string1 = df.to_json(orient='records')
    return df

def limitedDf_from_path(sourcepath,chunksize):
    '''Returns a pandas dataframe when given a source path'''
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]

    fileize= os.path.getsize(sourcepath)
    filesize = fileize/(1024*1024)
    if filesize>15:
        df=getchunkforPreview(sourcepath,chunksize)
    else:
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
                # df = pd.read_json(sourcepath, orient = "index")
        elif(file_ext==".xml"):
            try:
                df = pd.read_xml(sourcepath)
            except:
                err_msg = {"Error":"XML file read error. XML file has faulty structure which mismatches the reference example or\
                            XML file may contain column names with whitespaces or special characters other than '_' "}
                return err_msg
    df = force_datetime_conversion(df) ##Force DataType conversion to suitable date/time cols
   
    return df

def editCleaningSourceFromProfile(sourcedb,sourceId,sourcedetails):
    data = json.load(open("cleandb.json","r"))
    SourceList=  data['SourceDetailsList']
    df = pd.DataFrame.from_dict(SourceList)

    df = df.drop(df[df.originalsourceId==sourceId].index)
    SourceList=df.to_dict('records')
    sourcedetails["source"]=sourcedb
    sourceDataName=sourcedetails['sourceDataName']
    sourceCategory = sourcedetails['sourceCategory']
    db="clean"
    newSourceId = sourceDataName[0] + sourceDataName[-1] + '_CS'+ str(int(getPC_SourceMaxId(sourceCategory, db)+1))
    sourcedetails['sourceId']= newSourceId
    SourceList.append(sourcedetails)
    data['SourceDetailsList'] = SourceList
    json.dump(data, open("cleandb.json","w"), default=str)
    
    return "true"

def editProfileSourceFromcleaning(sourcedb,sourceId,sourcedetails):
    data = json.load(open("profiledb.json","r"))
    SourceList=  data['SourceDetailsList']
    df = pd.DataFrame.from_dict(SourceList)
    df = df.drop(df[df.originalsourceId==sourceId].index)
    SourceList=df.to_dict('records')
    sourcedetails["source"]=sourcedb
    sourceDataName=sourcedetails['sourceDataName']
    sourceCategory = sourcedetails['sourceCategory']
    db="clean"
    newSourceId = sourceDataName[0] + sourceDataName[-1] + '_CS'+ str(int(getPC_SourceMaxId(sourceCategory, db)+1))
    sourcedetails['sourceId']= newSourceId
    SourceList.append(sourcedetails)
    data['SourceDetailsList'] = SourceList

    json.dump(data, open("profiledb.json","w"), default=str)
    return "true"

def editcommondb(sourceId,sourcedetails):    
    with open('commondb.json', 'r') as openfile:
            rjson_object = json.load(openfile)

    rdata=rjson_object
    data= rdata['commonSourceList']
    df = pd.DataFrame.from_dict(data)

    
    df = df.drop(df[df.sourceId==sourceId].index)
    df2=sourcedetails
    df = df.append(df2, ignore_index = True)
    rdata={}

    if not df.empty:
        rdata['commonSourceList']=[]
        rdata['commonSourceList']=df.to_dict('records')
        json.dump(rdata, open("commondb.json","w"))
        rdata['responseMsg']= 'Department added successfully'
        rdata['errorflag']= 'False'
    else:
        rdata['errorMsg']= 'Department already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString

def df_from_path_uploadSource(sourcepath):
    '''Returns a pandas dataframe when given a source path'''
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    sqllitePath="sqlite:///" + os.path.splitext(os.path.basename(sourcepath))[0]+".db"
    fileize= os.path.getsize(sourcepath)
    filesize = fileize/(1024*1024)
    if filesize>15:
        df=getdffromsql(sqllitePath)
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

def corr_mat_highcharts(corr_mat_df):
  '''Takes a correlation matrix dataframe as input and returns a dictionary object in the format accepted by HighCharts'''
  columns = corr_mat_df.columns.to_list()
  matrix_len = len(corr_mat_df)
  corr_matrix_data = []
  for i in range(0, matrix_len):
    for j in range(0, matrix_len):
      corr_value = corr_mat_df.iloc[i,j]
      corr_val_list = [i,j,corr_value]
      corr_matrix_data.append(corr_val_list)
  return {'columns':columns,'matrix':corr_matrix_data }
