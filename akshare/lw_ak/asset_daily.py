# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 15:07:22 2020

@author: roger luo
"""

import akshare as ak

import pandas as pd

from akshare.lw_ak.utilis import _convert_code, iterate_for_code
from akshare.lw_ak.codeset import (stock_a_code, index_a_code, em_fund_code,
                                   index_us_code)


from functools import wraps

def asset_pool(key=None):
    '''
    dict of (codeset, single security asset daily hist func) pairs
    to retrieve data from public web resources

    Parameters
    ----------
    key : TYPE
        DESCRIPTION.

    Returns
    -------
    tuple :
        (codeset, func_to_run_code)

    '''

    
    d = {
        "stock_a" : (stock_a_code, stock_a_daily),
        "index_a" : (index_a_code, index_a_daily),
        "em_fund_a" : (em_fund_code, em_fund_daily),
        "index_us" : (index_us_code, index_us_daily),
        # "fin_indi" : (stock_a_code, ak.stock_financial_analysis_indicator)
         
         }
    
    dd = {}
    
    dd['stock_a'] = "China's a stock market daily data"
    
    if key is None:
        return dd
    elif key == -1:
        return d    
    else:
        return d[key]

@wraps(ak.stock_zh_a_daily)
def stock_a_daily(code, *args,**kwargs):
    '''
    '''
    code = _convert_code(ak.stock_zh_a_daily)(code)
    
    return ak.stock_zh_a_daily(code, *args, **kwargs)

@wraps(ak.stock_zh_index_daily)
def index_a_daily(code, *args, **kwargs):
    '''return stock a index daily data for given code symbol to latest datetime
    '''
    code = _convert_code(ak.stock_zh_index_daily)(code)
    return ak.stock_zh_index_daily(code, *args, **kwargs)

@wraps(ak.index_investing_global)
def index_us_daily(code, start_date='2000-01-01', 
                        country='美国', period='每日', **kwargs):
    '''
    return us index daily data for given code symbol to latest datetime
    
    '''
    import datetime
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    index_data = ak.index_investing_global(index_name=code, 
                                           country=country, 
                                           period=period,
                                           end_date=end_date,
                                            **kwargs)
    index_data.index.name = 'date'
    index_data.columns = ['close', 'open', 'high', 'low', 'volume']
    return index_data.reset_index()

def em_fund_daily(symbol):
    '''
    

    Parameters
    ----------
    symbol : TYPE
        fund_code.

    Returns
    -------
    ['净值日期', '单位净值', '累计净值', '日增长率', '申购状态', '赎回状态']

    '''
    
    return ak.fund_em_etf_fund_info(symbol).rename(columns={'净值日期':'date'})

if __name__ == '__main__':
    pass
    # iterate_for_code(*asset_pool('index_a'), test_num=1)
    iterate_for_code(*asset_pool('stock_a'), test_num=1)
    # iterate_for_code(*asset_pool('em_fund_a'), test_num=1)
    # iterate_for_code(*asset_pool('index_us'), test_num=1)
    # iterate_for_code(*asset_pool('fin_indi'), test_num=1)
    
    
