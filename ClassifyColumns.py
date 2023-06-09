import pandas as pd
import numpy as np
import copy
from functools import reduce



################################################################################
######### NEW And FAST WAY to CLASSIFY COLUMNS IN A DATA SET #######
##### SOURCE: https://github.com/AutoViML
#### LICENSE: APACHE
################################################################################

##################################################################################################
def left_subtract(l1,l2):
    lst = []
    for i in l1:
        if i not in l2:
            lst.append(i)
    return lst

#################################################################################
def marthas_columns(data,verbose=0):
    """
    This program is named  in honor of my one of students who came up with the idea for it.
    It's a neat way of printing data types and information compared to the boring describe() function in Pandas.
    """
    data = data[:]
    print('Data Set Shape: %d rows, %d cols' % data.shape)
    if data.shape[1] > 30:
        print('Too many columns to print')
    else:
        if verbose==1:
            print('Data Set columns info:')
            for col in data.columns:
                print('* %s: %d nulls, %d unique vals, most common: %s' % (
                        col,
                        data[col].isnull().sum(),
                        data[col].nunique(),
                        data[col].value_counts().head(2).to_dict()
                    ))
            print('--------------------------------------------------------------------')
##################################################################################

def classify_columns(df_preds, verbose=0):
    """
    Takes a dataframe containing only predictors to be classified into various types.
    DO NOT SEND IN A TARGET COLUMN since it will try to include that into various columns.
    Returns a data frame containing columns and the class it belongs to such as numeric,
    categorical, date or id column, boolean, nlp, discrete_string and cols to delete...
    ####### Returns a dictionary with 10 kinds of vars like the following: # continuous_vars,int_vars
    # cat_vars,factor_vars, bool_vars,discrete_string_vars,nlp_vars,date_vars,id_vars,cols_delete
    """
    train = copy.deepcopy(df_preds)
    #### If there are 30 chars are more in a discrete_string_var, it is then considered an NLP variable
    max_nlp_char_size = 30
    max_cols_to_print = 30
    print('############## C L A S S I F Y I N G  V A R I A B L E S  ####################')
    print('Classifying variables in data set...')
    #### Cat_Limit defines the max number of categories a column can have to be called a categorical colum
    cat_limit = 15
    float_limit = 8 #### Make this limit low so that float variables below this limit become cat vars ###
    def add(a,b):
        return a+b
    sum_all_cols = dict()
    orig_cols_total = train.shape[1]
    #Types of columns
    cols_delete = [col for col in list(train) if (len(train[col].value_counts()) == 1
                                   ) | (train[col].isnull().sum()/len(train) >= 0.90)]
    train = train[left_subtract(list(train),cols_delete)]
    var_df = pd.Series(dict(train.dtypes)).reset_index(drop=False).rename(
                        columns={0:'type_of_column'})
    sum_all_cols['cols_delete'] = cols_delete
    var_df['bool'] = var_df.apply(lambda x: 1 if x['type_of_column'] in ['bool','object']
                        and len(train[x['index']].value_counts()) == 2 else 0, axis=1)
    string_bool_vars = list(var_df[(var_df['bool'] ==1)]['index'])
    sum_all_cols['string_bool_vars'] = string_bool_vars
    var_df['num_bool'] = var_df.apply(lambda x: 1 if x['type_of_column'] in [np.uint8,
                            np.uint16, np.uint32, np.uint64,
                            'int8','int16','int32','int64',
                            'float16','float32','float64'] and len(
                        train[x['index']].value_counts()) == 2 else 0, axis=1)
    num_bool_vars = list(var_df[(var_df['num_bool'] ==1)]['index'])
    sum_all_cols['num_bool_vars'] = num_bool_vars
    ######   This is where we take all Object vars and split them into diff kinds ###
    discrete_or_nlp = var_df.apply(lambda x: 1 if x['type_of_column'] in ['object']  and x[
        'index'] not in string_bool_vars+cols_delete else 0,axis=1)
    ######### This is where we figure out whether a string var is nlp or discrete_string var ###
    var_df['nlp_strings'] = 0
    var_df['discrete_strings'] = 0
    var_df['cat'] = 0
    var_df['id_col'] = 0
    discrete_or_nlp_vars = var_df.loc[discrete_or_nlp==1]['index'].values.tolist()
    ##### This is complicated logic - do not touch #############################
    if len(var_df.loc[discrete_or_nlp==1]) != 0:
        for col in discrete_or_nlp_vars:
            #### first fill empty or missing vals since it will blowup ###
            train[col] = train[col].fillna('  ')
            if train[col].map(lambda x: len(x) if type(x)==str else 0).mean(
                ) >= max_nlp_char_size and len(train[col].value_counts()
                        ) <= int(0.9*len(train)) and col not in string_bool_vars:
                var_df.loc[var_df['index']==col,'nlp_strings'] = 1
            elif len(train[col].value_counts()) >= cat_limit and len(train[col].value_counts()
                        ) <= int(0.9*len(train)) and col not in string_bool_vars:
                var_df.loc[var_df['index']==col,'discrete_strings'] = 1
            elif len(train[col].value_counts()) >= cat_limit and len(train[col].value_counts()
                        ) == len(train) and col not in string_bool_vars:
                var_df.loc[var_df['index']==col,'id_col'] = 1
            elif len(train[col].value_counts()) < cat_limit and col not in string_bool_vars:
                var_df.loc[var_df['index']==col,'cat'] = 1
            else:
                if train[col].map(lambda x: len(x) if type(x)==str else 0).mean() >= max_nlp_char_size:
                    var_df.loc[var_df['index']==col,'nlp_strings'] = 1
                else:
                    var_df.loc[var_df['index']==col,'discrete_strings'] = 1
    ##### This is complicated logic - do not touch #############################
    nlp_vars = list(var_df[(var_df['nlp_strings'] ==1)]['index'])
    sum_all_cols['nlp_vars'] = nlp_vars
    discrete_string_vars = list(var_df[(var_df['discrete_strings'] ==1) ]['index'])
    sum_all_cols['discrete_string_vars'] = discrete_string_vars
    ###### This happens only if a string column happens to be an ID column #######
    #### DO NOT Add this to ID_VARS yet. It will be done later.. Dont change it easily...
    #### Category DTYPE vars are very special = they can be left as is and not disturbed in Python. ###
    var_df['dcat'] = var_df.apply(lambda x: 1 if str(x['type_of_column'])=='category' else 0,
                            axis=1)
    factor_vars = list(var_df[(var_df['dcat'] ==1)]['index'])
    sum_all_cols['factor_vars'] = factor_vars
    ########################################################################
    date_or_id = var_df.apply(lambda x: 1 if x['type_of_column'] in [np.uint8,
                         np.uint16, np.uint32, np.uint64,
                         'int8','int16',
                        'int32','int64']  and x[
        'index'] not in string_bool_vars+num_bool_vars+discrete_string_vars+nlp_vars else 0,
                                        axis=1)
    ######### This is where we figure out whether a numeric col is date or id variable ###
    var_df['int'] = 0
    var_df['date_time'] = 0
    ### if a particular column is date-time type, now set it as a date time variable ##
    var_df['date_time'] = var_df.apply(lambda x: 1 if x['type_of_column'] in ['<M8[ns]','datetime64[ns]']  and x[
        'index'] not in string_bool_vars+num_bool_vars+discrete_string_vars+nlp_vars else 0,
                                        axis=1)
    ### this is where we save them as date time variables ###
    if len(var_df.loc[date_or_id==1]) != 0:
        for col in var_df.loc[date_or_id==1]['index'].values.tolist():
            if len(train[col].value_counts()) == len(train):
                if train[col].min() < 1900 or train[col].max() > 2050:
                    var_df.loc[var_df['index']==col,'id_col'] = 1
                else:
                    try:
                        pd.to_datetime(train[col],infer_datetime_format=True)
                        var_df.loc[var_df['index']==col,'date_time'] = 1
                    except:
                        var_df.loc[var_df['index']==col,'id_col'] = 1
            else:
                if train[col].min() < 1900 or train[col].max() > 2050:
                    if col not in num_bool_vars:
                        var_df.loc[var_df['index']==col,'int'] = 1
                else:
                    try:
                        pd.to_datetime(train[col],infer_datetime_format=True)
                        var_df.loc[var_df['index']==col,'date_time'] = 1
                    except:
                        if col not in num_bool_vars:
                            var_df.loc[var_df['index']==col,'int'] = 1
    else:
        pass
    int_vars = list(var_df[(var_df['int'] ==1)]['index'])
    date_vars = list(var_df[(var_df['date_time'] == 1)]['index'])
    id_vars = list(var_df[(var_df['id_col'] == 1)]['index'])
    sum_all_cols['int_vars'] = int_vars
    copy_date_vars = copy.deepcopy(date_vars)
    for date_var in copy_date_vars:
        #### This test is to make sure sure date vars are actually date vars
        try:
            pd.to_datetime(train[date_var],infer_datetime_format=True)
        except:
            ##### if not a date var, then just add it to delete it from processing
            cols_delete.append(date_var)
            date_vars.remove(date_var)
    sum_all_cols['date_vars'] = date_vars
    sum_all_cols['id_vars'] = id_vars
    sum_all_cols['cols_delete'] = cols_delete
    ## This is an EXTREMELY complicated logic for cat vars. Don't change it unless you test it many times!
    var_df['numeric'] = 0
    float_or_cat = var_df.apply(lambda x: 1 if x['type_of_column'] in ['float16',
                            'float32','float64'] else 0,
                                        axis=1)
    if len(var_df.loc[float_or_cat == 1]) > 0:
        for col in var_df.loc[float_or_cat == 1]['index'].values.tolist():
            if len(train[col].value_counts()) > 2 and len(train[col].value_counts()
                ) <= float_limit and len(train[col].value_counts()) <= len(train):
                var_df.loc[var_df['index']==col,'cat'] = 1
            else:
                if col not in num_bool_vars:
                    var_df.loc[var_df['index']==col,'numeric'] = 1
    cat_vars = list(var_df[(var_df['cat'] ==1)]['index'])
    continuous_vars = list(var_df[(var_df['numeric'] ==1)]['index'])
    ########  V E R Y    I M P O R T A N T   ###################################################
    ##### There are a couple of extra tests you need to do to remove abberations in cat_vars ###
    cat_vars_copy = copy.deepcopy(cat_vars)
    for cat in cat_vars_copy:
        if df_preds[cat].dtype==float:
            continuous_vars.append(cat)
            cat_vars.remove(cat)
            var_df.loc[var_df['index']==cat,'cat'] = 0
            var_df.loc[var_df['index']==cat,'numeric'] = 1
        elif len(df_preds[cat].value_counts()) == df_preds.shape[0]:
            id_vars.append(cat)
            cat_vars.remove(cat)
            var_df.loc[var_df['index']==cat,'cat'] = 0
            var_df.loc[var_df['index']==cat,'id_col'] = 1
    sum_all_cols['cat_vars'] = cat_vars
    sum_all_cols['continuous_vars'] = continuous_vars
    sum_all_cols['id_vars'] = id_vars
    ###### This is where you consoldate the numbers ###########
    var_dict_sum = dict(zip(var_df.values[:,0], var_df.values[:,2:].sum(1)))
    for col, sumval in var_dict_sum.items():
        if sumval == 0:
            print('%s of type=%s is not classified' %(col,train[col].dtype))
        elif sumval > 1:
            print('%s of type=%s is classified into more then one type' %(col,train[col].dtype))
        else:
            pass
    ###############  This is where you print all the types of variables ##############
    ####### Returns 8 vars in the following order: continuous_vars,int_vars,cat_vars,
    ###  string_bool_vars,discrete_string_vars,nlp_vars,date_or_id_vars,cols_delete
    if verbose == 1:
        print("    Number of Numeric Columns = ", len(continuous_vars))
        print("    Number of Integer-Categorical Columns = ", len(int_vars))
        print("    Number of String-Categorical Columns = ", len(cat_vars))
        print("    Number of Factor-Categorical Columns = ", len(factor_vars))
        print("    Number of String-Boolean Columns = ", len(string_bool_vars))
        print("    Number of Numeric-Boolean Columns = ", len(num_bool_vars))
        print("    Number of Discrete String Columns = ", len(discrete_string_vars))
        print("    Number of NLP String Columns = ", len(nlp_vars))
        print("    Number of Date Time Columns = ", len(date_vars))
        print("    Number of ID Columns = ", len(id_vars))
        print("    Number of Columns to Delete = ", len(cols_delete))
    if verbose == 2:
        marthas_columns(df_preds,verbose=1)
        print("    Numeric Columns: %s" %continuous_vars[:max_cols_to_print])
        print("    Integer-Categorical Columns: %s" %int_vars[:max_cols_to_print])
        print("    String-Categorical Columns: %s" %cat_vars[:max_cols_to_print])
        print("    Factor-Categorical Columns: %s" %factor_vars[:max_cols_to_print])
        print("    String-Boolean Columns: %s" %string_bool_vars[:max_cols_to_print])
        print("    Numeric-Boolean Columns: %s" %num_bool_vars[:max_cols_to_print])
        print("    Discrete String Columns: %s" %discrete_string_vars[:max_cols_to_print])
        print("    NLP text Columns: %s" %nlp_vars[:max_cols_to_print])
        print("    Date Time Columns: %s" %date_vars[:max_cols_to_print])
        print("    ID Columns: %s" %id_vars[:max_cols_to_print])
        print("    Columns that will not be considered in modeling: %s" %cols_delete[:max_cols_to_print])
    ##### now collect all the column types and column names into a single dictionary to return!
    len_sum_all_cols = reduce(add,[len(v) for v in sum_all_cols.values()])
    if len_sum_all_cols == orig_cols_total:
        print('    %d Predictors classified...' %orig_cols_total)
        print('        This does not include the Target column(s)')
    else:
        print('No of columns classified %d does not match %d total cols. Continuing...' %(
                   len_sum_all_cols, orig_cols_total))
        ls = sum_all_cols.values()
        flat_list = [item for sublist in ls for item in sublist]
        if len(left_subtract(list(train),flat_list)) == 0:
            print(' Missing columns = None')
        else:
            print(' Missing columns = %s' %left_subtract(list(train),flat_list))
    return sum_all_cols
