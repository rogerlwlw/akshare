 # -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 17:41:21 2020

@author: Administrator
"""

import akshare as ak
import pandas as pd
import numpy as np
import requests
import random
import re
import demjson

from functools import reduce
from loguru import logger

def _basic_cleandata(df):
    """Try cleaning mapping value of df to numeric 
    
    convert all space string , like '-._-   ' to np.nan, 
    convert accounting number like 1,232,000 to 1232000 and percentage like
    10% to 0.1.
    
    Parameters
    ----------
    df : data frame.

    Returns
    -------
    df : data frame.

    """
    # -- remove '^\s*$' for each cell
    df = df.replace("^[-\_\s\.]*$", np.nan, regex=True)

    # convert accounting number to digits by remove ',' and percentages
    df = df.applymap(_convert_numeric)

    return df


def _convert_numeric(x_str):
    """check if x_str is accounting or percentage number
    
    convent them to float or integer
    
    integer code string starting with '0' will be ingored, treated as code str

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

        if re.match(numeric_pattern, x_str_r) is not None:
            if re.match(code_pattern, x_str_r) is None:
                try:
                    x_str_r = int(x_str_r)
                except:
                    x_str_r = float(x_str_r)

                if re.match('^.*\%$', x_str):
                    # for percentage
                    return x_str_r / 100
                else:
                    # not percentage
                    return x_str_r
    # return input x_str
    return x_str



def merge_dfs(df_list, how='inner', **kwargs):
    '''return merged  DataFrames of all in 'df_list'
    '''
    lamf = lambda left, right: pd.merge(left, right, how=how, **kwargs)
    return reduce(lamf, df_list)

def _convert_code(func):
    '''return func to convert code for given code function 
    '''
    convert_func = {'stock_zh_a_daily' : _prefix_stock_code,
                    'stock_zh_index_daily' : _prefix_index_code,
    }
       
    return convert_func.get(func.__name__, lambda x: x) 


def _prefix_stock_code(stock_code):
    '''
    add prefix to stock code symbol
    for shenzhen exchange, stoc_code starts with '6'
    '''
    
    stock_code = str(stock_code)
    
    if not re.match('^\d+$', stock_code):
        return stock_code
    
    if stock_code[0] == '6':
        return ''.join(['sh', stock_code])
    else:
        return ''.join(['sz', stock_code])

def _prefix_index_code(index_code):
    '''
    add prefix to index code symbol
    
    for shenzhen exchange, index_code starts with '3'
    '''
    index_code = str(index_code)

    if not re.match('^\d+$', index_code):
        return index_code
    
    if index_code[0] == '3':
        return ''.join(['sz', index_code])
    else:
        return ''.join(['sh', index_code])    
    
def iterate_for_code(code_set_func, 
                      func, 
                      test_num: int =-1,
                      max_try: int =10, 
                      **kwargs):
    """
    to iterate a set of code symbols to get daily prices data, then concatenate
    them to an entine dataframe.

    Parameters
    ----------
    code_set_func : set, array, sequence or func
        set of code to iterate
    func : func
        func that returns df data for each code. The first args of func must 
        be code
        
    test_num : int
        if test_num<0, run all codes returned by code_set_func
        if test_num>0, run sampled number (test_num) of codes

    max_try : int, optional
        max run times to try func if failed. The default is 10.
    
    **kwargs : TYPE
        key words passed to func.

    Returns
    -------
    data : df
        df table of all codes concatenated.

    """
    # get codeset
    if callable(code_set_func):
        code_set = code_set_func()
    else:
        code_set = set(code_set_func)
        
    if test_num > 0:
        code_set = set(random.sample(code_set, test_num))      
    
    data_list = []
    n = 0
    # try max_try times to run codes in codeset
    while len(code_set) > 0 and n < max_try:
        n += 1
        failed_stock_code = set()
        for code in code_set:
            code0 = _convert_code(func)(code)
            try:
                df = func(code0, **kwargs)
                if df is not None:
                    df['code'] = code
                    data_list.append(
                        df.reset_index().dropna().drop(columns=['index'], 
                                                       errors='ignore')
                        )
                    logger.info('{} {} downloaded..\n'.format(func.__name__, code))
            except (requests.Timeout, requests.RequestException, demjson.JSONError):
                failed_stock_code.add(code)
                logger.exception(
                    'Requests exception - {} when calling {}'.format(
                    code, func.__name__
                ))
            except:
                logger.exception(
                    'unexpected Exception - for {} when calling {}'.format(
                    code, func.__name__
                ))
                                
        code_set = failed_stock_code.copy()
    
    if len(code_set) > 0:
        logger.info("for {}, {} failed".format(func.__name__, 
                                               str(code_set)), 
                   exc_info=True)
    # concatenate table
    data = pd.concat(data_list, axis=0)  
    
    return _basic_cleandata(data)


