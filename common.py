
from sqlalchemy import create_engine, false, true
import pandas as pd
import json
import os
import os.path
from io import StringIO
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_datetime64_any_dtype
from werkzeug.utils import secure_filename
import cx_Oracle

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
    #df = force_datetime_conversion(df) ##Force DataType conversion to suitable date/time cols
    
    return df


def df_from_path(sourcepath):
    '''Returns a pandas dataframe when given a source path'''
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    print(file_ext)
    fileize= os.path.getsize(sourcepath)
    filesize = fileize/(1024*1024)
    print(filesize)
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

def get_df_RefSQLlite(db,tablename):
    '''get df from sqllite a database'''
    csv_database = create_engine('sqlite:///csv_database_ref.db')
    df = pd.read_sql_query('SELECT * FROM ['+tablename +']', csv_database)     
    
    return df

def getdffromsql(dbName):
    
    csv_database = create_engine(dbName)
    df = pd.read_sql_query('SELECT * FROM tblSource limit 3000000', csv_database)  
    df.drop('index', axis=1, inplace=True)  
    return df

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