# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 16:41:57 2020

@author: Administrator
"""


import akshare as ak
import inspect
import pandas as pd
import numpy as np
import re
import requests


import logging
import traceback

from pandas.core.dtypes import api

from sqlite_engine import get_sqlite_engine

from daily_data import (daily_a_index, 
                        daily_a_stock,
                        daily_index_us_main,
                        daily_em_fund,
                        daily_sina_financical_indicators,
                        
                        code_name_a_index,
                        code_name_us_main_index,
                        code_name_a_stock,
                        code_name_em_fund,
                              )

from stock_feature_engineering import (stock_index_comp,
                                       em_analyst_index,
                                       stock_institute_detail,
                                       stock_institute_hold,
                                       stock_sina_fundhold_hist,
                                       stock_rating,
                                       stock_rating_hist,                                       
                                       stock_cashlow100,
                                       )

from maco_feature_eng import a_market_pb, a_market_pe

from akshare_log import init_log

def get_download_tab_list():
    '''
    '''
    akshare_api_table=[

        
        ]    
    
    return


    
def get_flat_list(x):
    '''
    list and flatten object into one dimension list
    
    return
    ----
    one dimension list
    '''
    if isinstance(x, list) and not isinstance(x, (str, bytes)):
        return [a for i in x for a in get_flat_list(i)]
    else:
        return [x]

def basic_cleandata(df):
    """
    remove spaces string as np.nan;
    
    replace ',' in number to make it convertible to numeric data;
    

    Parameters
    ----------
    df : TYPE
        data frame.

    Returns
    -------
    data : TYPE
        data frame.

    """

    # -- remove '^/s*$' for each cell
    data = df.replace('^[-\s]*$', np.nan, regex=True)
    # convert accounting number to digits by remove ','
    data = data.applymap(_convert_numeric)
    return data

def _convert_numeric(x_str):
    """
    check if x_str is numeric convertible
    if true, replace ',' '%' in x_str and then convert it to numeric
    if false return x_str
    integer code string starting with '0' will be ingored

    Parameters
    ----------
    x_str : TYPE
        DESCRIPTION.

    Returns
    -------
    x_str : TYPE
        float(x_str) or x_str.

    """
    # str_pattern to represent numrical data
    numeric_pattern = "^[-+]?\d*(?:\.\d*)?(?:\d[eE][+\-]?\d+)?(\%)?$"
    # integer code starts with 0
    code_pattern = "^0\d+$"
    
    def _is_numeric_pattern(x_str):
        try:
            float(x_str)
            return True
        except:
            return False
        
    if isinstance(x_str, str):
        x_str_r = re.sub("[,\%\$]", '', x_str)
        if _is_numeric_pattern(x_str_r):
            if re.match(code_pattern, x_str_r) is None:
                try:
                    x_str_r =  int(x_str_r) 
                except:
                    x_str_r = float(x_str_r) 
            
                if  re.match('^.*\%$', x_str):
                    # for percentage
                    return x_str_r / 100
                else:
                    # not percentage
                    return x_str_r
    # return input x_str       
    return x_str
    
def to_num_datetime(col, name='array', thresh=0.75, **kwargs):
    '''convert col to numeric or datetime if possible, otherwise remain
    unchaged 
    
    parameters
    ----
    col --> series, scalar or ndarry will be turned into series type
    
    name --> name of the col series 
    
    thresh --> default 0.8 
        - if more than the thresh percentage of X could be converted, 
          then should commit conversion   
    **kwargs 
    
    - errors - {'ignore', 'raise', 'coerce'}, default --> 'coerce'
        - If 'raise', then invalid parsing will raise an exception
        - If 'coerce', then invalid parsing will be set as NaN
        - If 'ignore', then invalid parsing will return the input
    other pandas to_datetime key words
    
    return
    ----
    converted series or df
    '''
    try:
        col = pd.Series(col)
    except Exception:
        raise Exception('col must be 1-d array/list/tuple/dict/Series')
    
    if api.is_numeric_dtype(col):
        return col
    if api.is_datetime64_any_dtype(col):
        return col
    if api.is_categorical_dtype(col):
        return col
    if col.count() == 0:
        return col
    if col.astype(str).str.contains('^0\d+$').any():
        return col

    is_numeric_convertible = False
    not_null_count = col.count()

    try:
        num = pd.to_numeric(col, errors=kwargs.get('errors', 'coerce'))
        if num.count() / not_null_count >= thresh:
            col = num
            is_numeric_convertible = True
    except:
        pass
    if not is_numeric_convertible:
        params = {'errors': 'coerce', 'infer_datetime_format': True}
        params.update(kwargs)
        try:
            date = pd.to_datetime(col, **params)
            if pd.notnull(date).sum() / not_null_count >= thresh:
                col = date
            else:
                col = col.apply(lambda x: x if pd.isna(x) else str(x))
        except:
            pass

    return col

def to_num_datetime_df(X, thresh=0.75):
    '''convert each column to numeric or datetime if possible, otherwise remain
    unchanged 
    
    thresh --> default 0.8 
        - if more than the thresh percentage of col could be converted, 
          then should commit conversion     
    '''
    try:
        X = pd.DataFrame(X)
    except Exception:
        raise ValueError('X must be df or convertible to df')
    lamf = lambda x: to_num_datetime(x, name=x.name, thresh=thresh)
    rst = X.apply(lamf, axis=0, result_type='reduce')
    return rst

def get_kwargs(func, **kwargs):
    '''return subset of **kwargs that are of func arguments
    '''
    func_args = set(inspect.getfullargspec(func).args)
    func_args.intersection_update(kwargs)
    return {i: kwargs[i] for i in func_args}

    
def download_tab(df_func, engine_conn=None, max_try: int =10, **kwargs):
    '''
    
    call df_func to return df_data, and then upload df_data to database
    under tablename of "df_func.__name__"
    
    df_func: function or list of function
        function that returns a dataframe

    engine : TYPE
        data base connection URL.
        
    max_try : int, optional
        max run times to try calling df_func. The default is 10.
    
    **kwargs:
        key words passed to df_func
        
    Returns
    -------
    None.

    '''
    loger = init_log()
    
    if engine_conn is None:
        engine_conn = get_sqlite_engine()
        
    download_set = get_flat_list(df_func)
    # download_set = set(df_func_list)
    
    n = 0
    while len(download_set) > 0 and n < max_try:
        n += 1
        failed_code = []
        download_funcname = [i.__name__ for i in download_set]
        # logging_record
        msg = "try {} run times for function set {} \n".format(n, str(download_funcname))
        loger.info(msg)
        # download data returned by each func and upload to db
        for func_api in download_set:
            df = None
            try:
                # do some calculation
                params = get_kwargs(func_api, **kwargs)
                df = func_api(**params)
            except (requests.Timeout, requests.RequestException):
                failed_code.append(func_api)
                # logging_record
                loger.exception(
                    'Requests exception occured when calling {}'.format(
                    func_api.__name__)
                )
            except:
                loger.exception('unexpected Exception occured when calling {}'.format(
                    func_api.__name__))
                raise Exception()
                
            if df is not None:
                # clean data
                df = basic_cleandata(df)
                df = to_num_datetime_df(df, thresh=0.75)
                df.to_sql(
                            func_api.__name__,
                            engine_conn,
                            if_exists='replace',
                            index=False,
                            chunksize=10000,
                        )
                loger.info("upload table '{}' successfully \n".format(func_api.__name__))
        
        # rerun download_set         
        download_set = failed_code.copy() 
    
    if len(download_set) > 0:
        # logging_record
        loger.info("total of {} func_api failed to download as {}".format(
            len(download_set), [i.__name__ for i in download_set])) 
        
    logging.shutdown()
    return df


if __name__ == '__main__':
    import sqlalchemy
    from sqlite_engine import get_sqlite_engine
    engine_conn = get_sqlite_engine('D:/capital_market_new')
    engine_conn = sqlalchemy.create_engine(
        'sqlite:///D:/capital_market_new.db?check_same_thread=False', 
                                           echo=False)
    # download_tab(
    #     [
    #         daily_a_stock,
    #         code_name_a_stock,
    #         daily_sina_financical_indicators,
            
    #         daily_a_index, 
    #         code_name_a_index,
    #         stock_index_comp,
            
    #         daily_index_us_main,
    #         code_name_us_main_index,
            
    #         code_name_em_fund,
    #         daily_em_fund,
            
    #         ak.stock_em_analyst_rank,
    #         em_analyst_index,
            
    #     ], 
        
    #     engine_conn=engine_conn,
    #     test_num=-1,
    #     )
    
    download_tab(
        [
            # stock_institute_detail,
            # stock_institute_hold,
            # # stock_sina_fundhold_hist,
            
            # stock_rating,
            # stock_rating_hist,

            # stock_cashlow100,
            
            # ak.stock_em_jgdy_tj,
            # ak.stock_em_jgdy_detail,
            a_market_pb,
            a_market_pe,
        ], 
        engine_conn=engine_conn,
        test_num=-1,                 
        quarter='20200301')
    
    
