# -*- coding: utf-8 -*-
"""

Asset attributes could be added by analyst mannually for this module only
describe static labels of security asset

Created on Tue Oct 20 16:35:43 2020

@author: roger luo
"""

import akshare as ak

import pandas as pd


def a_stock_attr() -> pd.DataFrame:
    """
    code and attributes table for China's stock A market

    Returns
    -------
    stock_a : pd.DataFrame
        ["code", "name", 'listing_date', 'market']

    """
    
    stock_sh = ak.stock_info_sh_name_code(indicator="主板A股")
    stock_sh = stock_sh[["SECURITY_CODE_A", "SECURITY_ABBR_A", "LISTING_DATE"]]
    stock_sh.columns = ["stock_code", "name", 'listing_date']
    stock_sh['market'] = '上证' 

    stock_sz = ak.stock_info_sz_name_code(indicator="A股列表")
    stock_sz["A股代码"] = stock_sz["A股代码"].astype(str).str.zfill(6)
    stock_sz = stock_sz[["A股代码", "A股简称", "A股上市日期", "板块"]]
    stock_sz.columns = ["stock_code", "name", 'listing_date', 'market'] 
               
    stock_kcb = ak.stock_info_sh_name_code(
        '科创板')[['SECURITY_CODE_A', 'SECURITY_ABBR_A', "LISTING_DATE"]]
    stock_kcb.columns = ["stock_code", "name", 'listing_date']
    stock_kcb['market'] = '科创版'
    
    stock_a = pd.concat([stock_sh, stock_sz, stock_kcb], axis=0)
    
    return stock_a

def a_index_attr() -> pd.DataFrame:
    '''
    code and attributes table for China's A stock market index 
    
    Return 
    -------
    DataFrame:
        
        all China's A stock  market index 
        ['index_code', 'display_name', 'publish_date']

    '''

    return ak.index_stock_info()

def em_fund_attr():
    '''
    '''

    fund_type = ['债券指数',
                 '债券型',
                 '定开债券',
                 '固定收益',
                 
                 '混合型',
                 
                 '股票指数',
                 '股票型',
                 
                 '联接基金',
                 '混合-FOF',
                 '股票-FOF',
                 
                 'ETF-场内',
                 'QDII-ETF',
                 
                 'QDII',
                 'QDII-指数',
                 
                 '分级杠杆',
                 # '货币型',     
                 # '理财型',
                 # '保本型',
                 # '其他创新',
                 ]
    
    fund_set = ak.fund_em_fund_name().rename(
        columns={'基金代码': 'fund_code'})
    
    return fund_set[fund_set['基金类型'].isin(fund_type)]

