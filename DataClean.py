from flask import Blueprint,request, render_template, send_from_directory
from flask_cors import CORS
import pandas as pd
import json
from common_PC_Function import *
from dataLineage import EntryDataLineage ,dataLineageforcolumn  
from sklearn.impute import IterativeImputer
from sklearn.linear_model import SGDClassifier
from sklearn import linear_model

DataClean = Blueprint('DataClean',__name__)

@DataClean.route('/api/GetAllCleandBSourceList', methods=['POST'])
def GetAllCleandBSourceList():
    content = request.get_json()   
    print(content) 
    userName=content['userName']
    userCategory=content['userCategory']
    userDepartment= content['department']
    '''API to retrieve complete cleandb json log file'''
    data = json.load(open("cleandb.json","r"))
    
    cleandbsingleList=[]
    for listitem in data['SourceDetailsList']:
        if isprofileSourceValid(userName,userCategory,userDepartment,listitem) :
            cleandbsingleList.append(listitem)
    data={}
    data['SourceDetailsList'] = cleandbsingleList
    jsonString = json.dumps(data, default=str)
    return jsonString

@DataClean.route("/api/cleandb_log_query", methods=["POST"])
def cleandb_log_query():
    try:
        content = request.get_json()
        query_col = content["query_col"]
        query_val = content["query_val"]
        with open("cleaned_data/cleandb_log.json", "r") as read_file:
            data = json.load(read_file)
        ### Convert JSON data to list of dictionaries
    #     print(data["CleanLog"])
        proc_data = []
        for row in data["CleanLog"]:
            print(row,'\n')
            proc_data.append(row[0])
        df = pd.DataFrame(data = proc_data)
        df_result = df[df[query_col] == query_val]
        return df_to_json(df_result.reset_index(drop=True))
    except Exception as error:
        resp = {}
        resp["error"] = str(error)
        return resp


@DataClean.route("/api/data_impute", methods=["POST"])
def data_impute():
    """Impute missing values with mean, median, mode or other specific value"""
    content = request.get_json()
    sourcepath= content["sourcepath"]
    source_ID = content["sourceId"]
    upload_ID = content['uploadId']
    process_time = content["processTime"]
    col = content["column"]
    col_data_type = content["column_data_type"]
    imp_value = content["value"]
    # cus_value=0
    # logged_by="Admin"
    cus_value = content["custom_value"]
    logged_by =content["Logged_by"]
    source_filename = content["sourceFileName"]
    filename = secure_filename(source_filename)
    source_filename = os.path.splitext(filename)[0].split('_cleaned')[0]
    ## "MEAN", "MEDIAN", "MODE", "MIN", "MAX","IQR25","IQR75" for numeric cols.
    ## "MODE" or any other Unique value for cat_cols
    df = df_from_path(sourcepath)
    selectedColumns=[]
    selectedColumns=df.columns.tolist()
    #print(col,"- Missing Value Count:",df[col].isnull().sum())
    if col!="":
        No_of_Records=df[col].isnull().sum()
        changed_value=0    
        ChangedRows=df[df[col].isnull()].index.tolist()
    else:
         No_of_Records=1    
         changed_value=0    
         ChangedRows=0
    

    if "conditions" in list(content.keys()):
      query_cond = conditions_chainer(content["conditions"])
      df = df.query(query_cond)
      print("Queried Condition:", query_cond)
      print("Queried dataframe length:", len(df))
    if "row_indices" in list(content.keys()) and len(content["row_indices"]) != 0:
        row_idx = content["row_indices"]
        df = df.iloc[row_idx]
        print("Dataframe for selected row_idx:\n",df)
    
        
    
    if col_data_type == "Numeric":
        if imp_value == "MEAN":
            df[col].fillna(df[col].mean(), inplace = True)
            changed_value=df[col].mean()
        elif imp_value == "MEDIAN":
            df[col].fillna(df[col].median(), inplace = True)
            changed_value=df[col].median()
        elif imp_value == "IQR25":
            df[col].fillna(df[col].describe()["25%"], inplace = True)
            changed_value=df[col].describe()["25%"]
        elif imp_value == "IQR75":
            df[col].fillna(df[col].describe()["75%"], inplace = True)
            changed_value=df[col].describe()["75%"]
        elif imp_value == "MODE":
            df[col].fillna(df[col].mode()[0], inplace = True)
            changed_value=df[col].mode()[0]
        elif imp_value == "MIN":
            df[col].fillna(df[col].min(), inplace = True)
            changed_value=df[col].min()
        elif imp_value == "MAX":
            df[col].fillna(df[col].max(), inplace = True)
            changed_value=df[col].max()
        elif imp_value =="OTHERVALUE":
            changed_value=cus_value
            df[col].fillna(cus_value, inplace = True)
        
    elif col_data_type == "Alphabetic":
        if imp_value == "MODE":
            df[col].fillna(df[col].mode()[0], inplace = True)
            changed_value=df[col].mode()[0]
        elif imp_value =="OTHERVALUE":
            changed_value=cus_value
            df[col].fillna(cus_value, inplace = True)
               
        else:
            changed_value=imp_value
            df[col].fillna(imp_value, inplace = True)
    elif col_data_type =="Alphanumeric":
        if imp_value =="OTHERVALUE":
            changed_value=cus_value
            df[col].fillna(cus_value, inplace = True)
            
        else:
            changed_value=imp_value
            df[col].fillna(imp_value, inplace = True)
    imp_action = "IMPUTE-{}-{}".format(col,imp_value)
    if imp_value=="ROW" :
            rowdetails = content["detail"]
            for rowdetail in rowdetails:
                rowid=rowdetail["ROW_ID"]
                print(rowdetail)
                for listKey in list(rowdetail.keys()):
                    if listKey != "ROW_ID":
                        #row_id=row_id+2
                        df.loc[int(rowid)-2,listKey]=rowdetail[listKey]   
                imp_action = "IMPUTE-the whole row of row index {}".format(rowid)
    
    #print(col,"- Missing Value Count:",df[col].isnull().sum())
    output_filename = "{}_cleaned.csv".format(source_filename)
    output_path = "cleaned_data/{}".format(output_filename)
    df.to_csv(output_path, index = False)
    changed_column=col
    previous_value="Null"
    changed_value=changed_value
    ## Save actions to log for audit trail
    data = json.load(open("cleaned_data/cleandb_log.json","r"))
    log_old =  data['CleanLog']
    new_log_data = [{"sourcepath": sourcepath, "sourceId" : source_ID, "sourceFileName": source_filename,
                   "uploadId": upload_ID, "processTime":process_time,
                    "outputFileName":output_filename, "outputpath":output_path, "action": imp_action,"ColumnChanged":changed_column,
                    "CleanedBy":logged_by,"PreviousValue":previous_value,"ChangedValue":changed_value,"ChangedRecordsCount":No_of_Records,"ChangedRows":ChangedRows}]
    log_old.append(new_log_data)
    data['CleanLog']= log_old
    json.dump(data, open("cleaned_data/cleandb_log.json","w"), default=str)
    messagel = "Cleaning Completed- IMPUTED from-{}-{}".format(col,imp_value)
    EntryDataLineage(sourcepath,source_ID,  source_filename,process_time, process_time,"cleaning",messagel,selectedColumns)
    
    return {"result": "Imputation SUCCESS", "outputpath":output_path, "outputFileName":output_filename } #df_to_json(df)

@DataClean.route("/api/nan_df_preview", methods=["POST"])
def nan_df_preview():
  '''Returns dataframe with row indices where a column value = NaN'''
  content = request.get_json()
  sourcepath = content["sourcepath"] # source dataframe
  col = content["column_name"] ## column to query in
  df = df_from_path(sourcepath)
  df_nan = df[df[col].isnull()]
  return df_to_json(df_nan)


@DataClean.route("/api/show_remove_duplicates", methods=["POST"])
def show_remove_duplicates():
    """Function to show all duplicates"""
    selectedColumns=[]
    content = request.get_json()
    sourcepath= content["sourcepath"]
    action = content["action"]
    subset_cols = content["select_cols"] ## List of col_names to show a subset
    keep = content["keep"] ## keep first, last or both
    ####
    source_ID = content["sourceId"]
    upload_ID = content['uploadId']
    process_time = content["processTime"]
    source_filename = content["sourceFileName"]
    ####
    filename = secure_filename(source_filename)
    source_filename = os.path.splitext(filename)[0].split('_cleaned')[0]
    df = df_from_path(sourcepath)
    selectedColumns=df.columns.tolist()
    print("Keep:",keep)
    if content["keep"] == "":
        keep = False
    if content["select_cols"] == "":
        subset_cols = df.columns.to_list()
    if action == "preview":
        df_result = df[df.duplicated(subset = subset_cols,keep = keep)]
        rows_dropped = df_result.index.to_list()
        print("Rows getting Dropped:",rows_dropped)
        return df_to_json(df_result)
    elif action == "remove_duplicates":
        df_result = df.copy()
        source_df_index = df.index.to_list()
        df_result.drop_duplicates(subset = subset_cols, keep = keep, inplace = True, ignore_index = False )
        result_df_index = df_result.index.to_list()
        output_filename = "{}_cleaned.csv".format(source_filename)
        output_path = "cleaned_data/{}".format(output_filename)
        df_result.to_csv(output_path, index = False)
        rows_dropped = list(set(source_df_index).difference(set(result_df_index)))
        print("Rows dropped:",rows_dropped)
        ## Save action to log for audit trail
        data = json.load(open("cleaned_data/cleandb_log.json","r"))
        log_old =  data['CleanLog']
        new_log_data = [{"sourcepath": sourcepath, "sourceId" : source_ID, "sourceFileName": source_filename,
                         "uploadId": upload_ID, "processTime":process_time,"outputFileName": output_filename, "outputpath":output_path,
                         "action": "REMOVED_DUPLICATES - Row indices - {}".format(rows_dropped)}]
        log_old.append(new_log_data)
        data['CleanLog']= log_old

        json.dump(data, open("cleaned_data/cleandb_log.json","w"), default=str)
        
        message = "SUCCESS - DUPLICATES removed"
        messagel = "Cleaning Completed-  DUPLICATES removed"
        EntryDataLineage(sourcepath,source_ID,  source_filename,process_time, process_time,"cleaning",messagel,selectedColumns)
        return {'result':message, "outputpath":output_path,"outputFileName":output_filename, 'row_indices_removed': rows_dropped }

@DataClean.route("/api/clean_data_save", methods=["POST"])
def clean_data_save():
    '''Saves the copy of the cleaned dataframe when user clicks the save button'''
    content = request.get_json()
    source_ID = content["sourceId"]
    # source_dataname = content["sourceDataName"]
    # source_category = content["sourceCategory"]
    source_path = content["sourcePath"]
    upload_ID = content['uploadId']
    upload_time = content["uploadTime"]
    output_filename = content["outputFileName"]
    source_data_name = os.path.splitext(output_filename)[0]
    output_path = "{}/{}".format(content["outputPath"],output_filename)
    canoverwrite="No"
    if "canOverwrite" in content:
        canoverwrite=content['canOverwrite']

    if canoverwrite== "YES":
        print('replacing the original verion')
    else:
        if os.path.exists(output_path):
            content['errorMsg']= 'The filename Already exist'
            content['errorflag']= 'True'
            content['errorCode']= '108'
            return json.dumps(content)
    #####
    df = df_from_path(source_path)
    
    print("Saving Cleaned data file")
    df.to_csv(output_path, index = False)
    ####
    data = json.load(open("cleandb.json","r"))
    proc_data = []
    newcleanSourceId=""
    if upload_ID == source_ID:
        new_log_data = {"sourceDataName":source_data_name,"outputFileName": output_filename, "outputPath":output_path, "uploadTime":upload_time,"RulesetId":source_ID+"R1","cleanSourceId":newcleanSourceId}
    else:
        new_log_data = {"sourceDataName":source_data_name,"outputFileName": output_filename, 
        "outputPath":output_path, "uploadTime":upload_time, "uploadId": upload_ID,
        "RulesetId":source_ID+"R1","cleanSourceId":newcleanSourceId}
    
    for row in data["SourceDetailsList"]:
        if row["sourceId"] == source_ID or row["originalsourceId"] == source_ID:
            row['rules']=getSourceRules(source_ID)
            if canoverwrite!= "YES":
                if "CleanedFilesLog" in row.keys():
                    old_clean_data = row['CleanedFilesLog']
                    new_log_data["cleanSourceId"]="CL" + getcleanMaxId(old_clean_data)
                    old_clean_data.append(new_log_data)
                    row['CleanedFilesLog'] = old_clean_data
                else:
                    old_clean_data=[]
                    #new_log_data["cleanSourceId"]="CL" + getcleanMaxId(old_clean_data)
                    row['CleanedFilesLog'] = []
        proc_data.append(row)
    data['SourceDetailsList'] = proc_data
    json.dump(data, open("cleandb.json","w"))
    
    
    return data

@DataClean.route("/api/data_remove_preview", methods=["POST"])
def data_remove_preview():
    """
    Function to remove NaNs, selected rows or columns
    """
    content = request.get_json()
    category = content["category"] ## Values for Category: "row", "row_mask","col_complete","col_with_value", "row_nan", "col_nan"
    cols = content["column_name"] ## List of col_names if category == 'col_complete' or 'col_with_value'
    values = content["values"] ## List of row_numbers if category == row  or column value if category == json
    formula = content["formula"]
    threshold = content["threshold"] ## nr. of NaNs across rows or columns in %
    sourcepath= content["sourcepath"]
    df = df_from_path(sourcepath)
    cols_source = df.columns.to_list()
    nr_rows_prior = df.shape[0]
    nr_cols_prior = df.shape[1]
    if category == "row" or category == "row_mask":
        df_result = df.iloc[values]
    elif category == "col_with_value":
        if len(formula) != 0:
            df_result = numeric_col_relational_query(df, cols, formula )
        else:
            df_result = df[df[cols].isin(values)]
    elif category == "col_complete":
        df_result = df[df[cols]]
    elif category == "row_nan":
        na_free = df.dropna(axis = 0,how = "all")
        df_result = df[~df.index.isin(na_free.index)]
        print('length of na_free:',len(na_free),'df_result:\n',df_result)
        if threshold != "":
            thresh_val = int(threshold * len(df.columns.to_list()) )
            print("Considering NaNs across {} columns per row".format(thresh_val))
            na_free = df.dropna(axis = 0, thresh=thresh_val)
            df_result = df[~df.index.isin(na_free.index)]
            print('Nr. of rows with NaN:',len(na_free),'df_result:\n',df_result)
    elif category == "col_nan":
        na_free = df.dropna(axis = 1,  how = "all")
        df_result = df[~df.index.isin(na_free.index)]
        print('length of na_free:',len(na_free),'df_result:\n',df_result)
        if threshold != "":
            print("Percentage of NaNs considered per column {}%".format(threshold*100))
            df_result = df.loc[:, df.isnull().mean() <= threshold]
            print('length of df_result:',len(df_result),"\nNr. of cols:",df_result.shape[1], '\ndf_result Info:')
            df_result.info()
    nr_rows_post = df_result.shape[0]
    nr_cols_post = df_result.shape[1]
    cols_removed = list(set(cols_source).difference(set(df_result.columns)))
    print("Cols Removed:",cols_removed)
    print("Nr_rows_post:",nr_rows_post,"\nNr_cols_post:",nr_cols_post)
    # result = json.loads(df_to_json(df_result))
    if category == "col_nan":
        result = json.loads(df_to_json(df))
    else:
        result = json.loads(df_to_json(df_result))
    # print("Result (after df_to_json):", result)
    result.update({"nr_rows_prior":nr_rows_prior,"nr_cols_prior":nr_cols_prior,"nr_rows_post":nr_rows_post,"nr_cols_post":nr_cols_post,"cols_removed":cols_removed})
    # print("Result Final:", result)
    return result

@DataClean.route("/api/data_remove", methods=["POST"])
def data_remove():
    """
    Function to remove NaNs, selected rows or columns
    """
    content = request.get_json()
    sourcepath= content["sourcepath"]
    category = content["category"] ## Values for Category: "row", "row_mask","col_complete","col_with_value", "row_nan", "col_nan"
    cols = content["column_name"] ## List of col_names if category == 'col_complete' or 'col_with_value'
    values = content["values"] ## List of row_numbers if category == row  or column value if category == json
    formula = content["formula"] ## Formula is gven for numeric cols only
    threshold = content["threshold"] ## nr. of NaNs across rows or columns in %
    source_ID = content["sourceId"]
    upload_ID = content['uploadId']
    process_time = content["processTime"]
    source_filename = content["sourceFileName"]
    filename = secure_filename(source_filename)
    source_filename = os.path.splitext(filename)[0].split('_cleaned')[0]
    df = df_from_path(sourcepath)
    cols_source = df.columns.to_list()
    selectedColumns=cols_source
    nr_rows_prior = df.shape[0]
    nr_cols_prior = df.shape[1]
    if category == "row" or category == "row_mask":
        df.drop(values, axis = 0, inplace = True)
    elif category == "col_with_value":
        if len(formula) != 0: ## formula is given for numeric cols only
            df_range_query = numeric_col_relational_query(df, cols, formula )
            row_idx = df_range_query.index.to_list()
            df.drop(row_idx, axis = 0, inplace = True)
        else:
            row_idx = df[df[cols].isin(values)].index.to_list()
            df.drop(row_idx, axis = 0, inplace = True)
        print("{} values after dropping:".format(cols),df[cols].to_list())
    elif category == "col_complete":
        df.drop(cols, axis = 1, inplace = True)
    if category == "row_nan":
        df.dropna(axis = 0, inplace = True, how = "all")
        if threshold != "":
            thresh_val = int(threshold * len(df.columns.to_list()) )
            print("Considering NaNs across {} columns per row".format(thresh_val))
            df.dropna(axis = 0, inplace = True, thresh=thresh_val)
    if category == "col_nan":
        df.dropna(axis = 1, inplace = True, how = "all")
        if threshold != "":
            print("Considering NaNs across {} rows".format(threshold * df.shape[0]))
            df = df.loc[:, df.isnull().mean() <= threshold]
    output_filename = "{}_cleaned.csv".format(source_filename)
    output_path = "cleaned_data/{}".format(output_filename)
    df.to_csv(output_path, index = False)
    cols_post = df.columns.to_list()
    cols_removed = list(set(cols_source).difference(set(cols_post)))
    if len(formula) != 0:
        drop_action = "RANGEDROP - {} {} {} {} {}".format(cols, formula["operator1"],  formula["cond_value1"], formula["operator2"], formula["cond_value2"])
    elif values != "":
        drop_action = "DROP - {} - {} - {}".format(category, cols, values)
    else:
        drop_action = "DROP - {} Threshold = {}%. Columns Removed: {}".format(category, threshold*100, cols_removed)
    nr_rows_post = df.shape[0]
    nr_cols_post = df.shape[1]
    print("Cols Removed:",cols_removed)
    print("Nr_rows_prior:",nr_rows_prior,"\nNr_cols_prior:",nr_cols_prior)
    print("Nr_rows_post:",nr_rows_post,"\nNr_cols_post:",nr_cols_post)
    ## Save actions to log for audit trail
    data = json.load(open("cleaned_data/cleandb_log.json","r"))
    log_old =  data['CleanLog']
    new_log_data = [{"sourcepath": sourcepath, "sourceId" : source_ID, "sourceFileName": source_filename,
                   "uploadId": upload_ID, "processTime":process_time, "outputFileName":output_filename, "outputpath":output_path, "action": drop_action}]
    log_old.append(new_log_data)
    data['CleanLog']= log_old
    json.dump(data, open("cleaned_data/cleandb_log.json","w"), default=str)

    messagel = "Cleaning Completed- Data removed from-{}".format(str(cols_removed))
    EntryDataLineage(sourcepath,source_ID,  source_filename,process_time, process_time,"cleaning",messagel,selectedColumns)

    return {"result": "Data Dropping SUCCESS", "outputpath":output_path, "outputFileName":output_filename, "cols_removed":cols_removed,
            "nr_rows_prior":nr_rows_prior,"nr_cols_prior":nr_cols_prior,"nr_rows_post":nr_rows_post,"nr_cols_post":nr_cols_post}

@DataClean.route("/api/find_preview", methods=["POST"])
def find_preview():
  """Preview function complement to find_replace API"""
  content = request.get_json()
  sourcepath = content["sourcepath"]
  find_value = content["find_value"]
  df = df_from_path(sourcepath)
  #df_result = df[df.eq(find_value).any(1)]
  df = df.where(pd.notnull(df), 'None')
  df_result= df[
        df.apply(
            lambda column: column.astype(str).str.contains(find_value, regex=True, case=False, na=False)
        ).any(axis=1)
    ]
  print("Len of df_result:",len(df_result), "Queried Dataframe:\n",df_result)
  return df_to_json(df_result)

  
@DataClean.route("/api/find_replace", methods=["POST"])
def find_replace():
    selectedColumns=[]
    """Function to find and replace values in a dataframe"""
    content = request.get_json()
    sourcepath= content["sourcepath"]
    find_value = content["find_value"]
    replace_value = content["replace_value"]
    source_ID = content["sourceId"]
    upload_ID = content['uploadId']
    process_time = content["processTime"]
    source_filename = content["sourceFileName"]
    ####
    filename = secure_filename(source_filename)
    source_filename = os.path.splitext(filename)[0].split('_cleaned')[0]
    df = df_from_path(sourcepath)
    selectedColumns=df.columns.tolist()
    df_result = df.astype(str).replace(to_replace = find_value, value = replace_value)
    
    output_filename = "{}_cleaned.csv".format(source_filename)
    output_path = "cleaned_data/{}".format(output_filename)
    df_result.to_csv(output_path, index = False)

    ## Save action to log for audit trail
    data = json.load(open("cleaned_data/cleandb_log.json","r"))
    log_old =  data['CleanLog']
    new_log_data = [{"sourcepath": sourcepath, "sourceId" : source_ID, "sourceFileName": source_filename,
                     "uploadId": upload_ID, "processTime":process_time,"outputFileName": output_filename, "outputpath":output_path,
                     "action": "FIND_REPLACE - REPLACED {} with {} ".format(find_value,replace_value)}]
    log_old.append(new_log_data)
    data['CleanLog']= log_old
    json.dump(data, open("cleaned_data/cleandb_log.json","w"), default=str)
    message = "SUCCESS - FIND_REPLACE - REPLACED {} with {}".format(find_value,replace_value)
    messagel = "Cleaning Completed- REPLACED {} with {}".format(find_value,replace_value)
    EntryDataLineage(sourcepath,source_ID,  source_filename,process_time, process_time,"cleaning",messagel,selectedColumns)
    return {'result':message, "outputpath":output_path, "outputFileName":output_filename }

@DataClean.route("/api/df_to_json_preview", methods=["POST"])
def df_to_json_preview():
    """Function to quickly preview a dataframe stored in the backend"""
    content = request.get_json()
    sourcepath= content["sourcepath"]
    if "seeMoreEnabled" in content:
        seeMoreEnabled= content["seeMoreEnabled"]
    else:	
        seeMoreEnabled= "No"	
    if(seeMoreEnabled=="YES"):	
        df=df_from_path(sourcepath)	
    else:	
        df = limitedDf_from_path(sourcepath,1000)
    if type(df) == dict and "Error" in df.keys():
        return df
    else:
        resDf = df.where(pd.notnull(df), 'None')
        data_dict = resDf.to_dict('index')
        result={}
        result['Preview'] = data_dict
        result['Top'] = Top_three_percentage(df)
        jsonString = json.dumps(result, default=str)
    return jsonString


@DataClean.route("/api/ml_data_impute", methods=["POST"])
def ml_data_impute():
    """Impute missing values using ML algorithm"""
    content = request.get_json()
    sourcepath= content["sourcepath"]
    source_ID = content["sourceId"]
    upload_ID = content['uploadId']
    process_time = content["processTime"]
    selectedColumns = content["selectedColumns"]
    source_filename = content["sourceFileName"]
    filename = secure_filename(source_filename)
    source_filename = os.path.splitext(filename)[0].split('_cleaned')[0]
    df = df_from_path(sourcepath)

    col_final_list = df.columns.to_list()
    cols_not_imputed = df.columns.to_list()
    cols_not_imputed = [col for col in cols_not_imputed if col not in selectedColumns]

    # split categorical and continuous cols for different imputation processes
    df_meta = pd.DataFrame(df.dtypes).reset_index()
    df_meta.columns = ["column_name", "dtype"]
    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
    conFeat = df_meta[df_meta.dtype == "int64"].column_name.tolist() + df_meta[df_meta.dtype == "float64"].column_name.tolist()
    catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()

    # pick the required columns for ML based imputation
    selectedCatCol = [col for col in catFeat if col in selectedColumns]
    selectedConCol = [col for col in conFeat if col in selectedColumns]
    df_conFeat = df.filter(selectedConCol, axis=1).copy()
    df_catFeat = df.filter(selectedCatCol, axis=1).copy()

    # ML Imputation for continuous columns (MICE technique)
    conFeat_imputer = IterativeImputer(estimator=linear_model.BayesianRidge(), n_nearest_features=None, imputation_order='ascending')
    df_conFeat_imputed = pd.DataFrame(conFeat_imputer.fit_transform(df_conFeat), columns=df_conFeat.columns)
    # ML based result will always be in float. Convert them to Integer if the column is int in original dataset
    for col in selectedColumns:
        if df[col].dtype == 'int64':
            df_conFeat_imputed[col] = df_conFeat_imputed[col].astype('int64')
    
    # Imputation for Categorical data
    df_catFeat_imputed = pd.DataFrame()
    for col in selectedCatCol:
        df_catFeat_imputed[col] = df_catFeat[col].fillna(df_catFeat[col].mode()[0])
    
    # Concatenate unselected cols and imputed cols
    df_final = pd.concat([df[cols_not_imputed], df_conFeat_imputed, df_catFeat_imputed], axis=1)
    df_result = df_final[col_final_list]

    imp_action = "IMPUTE USING ML FOR - {}".format(selectedColumns)
    output_filename = "{}_cleaned.csv".format(source_filename)
    output_path = "cleaned_data/{}".format(output_filename)
    df_result.to_csv(output_path, index = False)

    ## Save actions to log for audit trail
    data = json.load(open("cleaned_data/cleandb_log.json","r"))
    log_old =  data['CleanLog']
    new_log_data = [{"sourcepath": sourcepath, "sourceId" : source_ID, "sourceFileName": source_filename,
                   "uploadId": upload_ID, "processTime":process_time,
                    "outputFileName":output_filename, "outputpath":output_path, "action": imp_action}]
    log_old.append(new_log_data)
    data['CleanLog']= log_old
    json.dump(data, open("cleaned_data/cleandb_log.json","w"), default=str)
    print(data)
    messagel = "Cleaning Completed- IMPUTE USING ML FOR - {}".format(selectedColumns)
    EntryDataLineage(sourcepath,source_ID,  source_filename,process_time, process_time,"cleaning",messagel,selectedColumns)
    return {"result": "Imputation SUCCESS", "outputpath":output_path, "outputFileName":output_filename }

@DataClean.route("/api/list_data_impute_records",methods=["POST"])
def list_data_impute_records():
    """Function to list the records that was imputed"""
    
    try:
        content=request.get_json()
        outputpath= content["outputpath"]
        source_ID = content["sourceId"]
        output_filename = content['outputfilename']
        rows_list=content['rowsList']
        df = df_from_path(outputpath)
        resdata={}
        sourData={}
        df_dict=[]
        r_list=[]
        if(df.empty):
            print("file is empty")
            resdata['errorMsg']= 'The filename is empty'
            resdata['errorflag']= 'True'
            resdata['errorCode']= '108'
        else:
            if(len(rows_list)>0):
                r_list=eval(rows_list)
            else:
                resdata['errorMsg']= 'The rows list is empty'
                resdata['errorflag']= 'True'
                resdata['errorCode']= '108'
            df_dict = df[df.index.isin(r_list)].to_dict()
            sourData['CleanSource']=df_dict
            print(df_dict)
            json_str = json.dumps(sourData,default=str)
            # print(json_str)
        return json_str
    except Exception as error:
        resdata['errorMsg']= str(error)
        resdata['errorflag']= 'True'
        resdata['errorCode']= '108'
        return json.dumps(resdata)