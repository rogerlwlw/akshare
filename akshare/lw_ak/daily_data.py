# -*- coding: utf-8 -*-
"""
to capture China's stock A market prices and index data 
and US stock market index data, then store them in sqlite database.

equity_daily : ['code', 'high', 'low', 'open', 'close', 'volume', 
                'outstanding_share']

equity_info : ['code', 'symbol', 'market', 'sector', 'is_AH', ]

equity_index_daily

equity_index_info

Created on Sat May 30 16:53:37 2020

@author: rogerluo
"""

import akshare as ak
import pandas as pd 
import random
import requests

from functools import reduce
from akshare_log import init_log

from utilis import (_iterate_for_code,
                    us_main_index_daily,
                    fund_em_fund_daily,
                    )

def daily_a_stock(max_try: int =50, test_num: int =-1):
    """
    try download stock pricing data for all securities in China's A stock
    market     

    Parameters
    ----------

    max_try : int, optional
        max run times to try for_iterations. The default is 50.
    test_num : int, optional
        if test_num > 0 then sample 'test_num' code to download. 
        The default is -1.

    Returns
    -------
    stock_a : pd.DataFrame.
        ['date', 'open', 'high', 'low', 'close', 'volume', 'outstanding_share',
         'turnover']

    """
    stock_data = _iterate_for_code(a_stock_codeset, 
                                   ak.stock_zh_a_daily, 
                                   test_num,
                                   max_try=max_try, 
                                   adjust='qfq')    
    
    return stock_data

def daily_a_index(max_try: int =50, test_num: int =-1):
    """
    try download stock index pricing data for all securities in China's 
    A stock market     

    Parameters
    ----------

    max_try : int, optional
        max run times to try for_iterations. The default is 50.
    test_num : int, optional
        if test_num > 0 then sample 'test_num' code to download. 
        The default is -1.

    Returns
    -------
    stock_a : DataFrame

    """

    index_data = _iterate_for_code(a_stock_index_codeset, 
                                   ak.stock_zh_index_daily,
                                   test_num,
                                   max_try=max_try)       
      
    return index_data

 
def daily_index_us_main(max_try: int =50, 
                           test_num: int =-1,
                           start_date='2000-01-01'):
    '''
    try download us main index pricing data 

    Parameters
    ----------
    max_try : int, optional
        DESCRIPTION. The default is 50.
    test_num : int, optional
        DESCRIPTION. The default is -1.
        
    start_date:
        starting time point (end time will be current datetime)

    Returns
    -------
    None.

    '''
    index_data = _iterate_for_code( us_main_index_codeset, 
                                    us_main_index_daily, 
                                    test_num,
                                    max_try=max_try, 
                                    start_date=start_date)  
    
    index_data.columns = ['date', 'close', 'open', 'high', 
                           'low', 'vol', 'code']
  
    return index_data


def daily_em_fund(max_try: int =50, test_num: int =-1):
    '''
    Parameters
    ----------
    max_try : int, optional
        DESCRIPTION. The default is 50.
    test_num : int, optional
        DESCRIPTION. The default is -1.
        
    return 
    ----------
    dataframe 
    '''
    
    fund_data = _iterate_for_code(em_fund_codeset, 
                                  fund_em_fund_daily, 
                                  test_num, 
                                  max_try=max_try)    
    
    return  fund_data

def daily_sina_financical_indicators(max_try: int =50, test_num: int =-1):
    '''
    
    2,968,140,000.00元, 需要处理

    
    return
    ------
    
    financial indicators for each quarter
    
    '''
    # --
        
    financial_data = _iterate_for_code(a_stock_codeset, 
                                       ak.stock_financial_analysis_indicator, 
                                       test_num,
                                       max_try=max_try)        
    return financial_data


def code_name_a_index() -> pd.DataFrame:
    '''
    Return DataFrame
    -----
    
    all China's A stock index 
    ['index_code', 'display_name', 'publish_date']

    '''

    return ak.index_stock_info()

def code_name_em_open_fund():
    '''
    '''
    return ak.fund_em_open_fund_daily().rename(
            columns={'基金代码' : 'fund_code'})

def code_name_em_fund():
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
    
    fund_set = ak.fund_em_fund_name().rename(columns={'基金代码': 'fund_code'})
    
    return fund_set[fund_set['基金类型'].isin(fund_type)]

def code_name_us_main_index():
    '''
    	US index
    0	纳斯达克100
    1	纳斯达克综合指数
    2	标普500指数
    3	道琼斯指数
    4	DJ Basic Materials
    5	DJ Consumer Goods
    6	DJ Consumer Services
    7	DJ Financials
    8	DJ Health Care
    9	DJ Industrials
    10	DJ Oil & Gas
    11	DJ Technology
    12	DJ Telecommunications
    13	DJ Utilities
    14	NASDAQ Bank
    15	NASDAQ Biotechnology
    16	NASDAQ Computer
    17	NASDAQ Financial 100
    18	NASDAQ Health Care
    19	NASDAQ Industrial
    20	NASDAQ Insurance
    21	NASDAQ Internet
    22	NASDAQ Other Finance
    23	NASDAQ Telecommunications
    24	NASDAQ Transportation
    25	NYSE Energy
    26	NYSE Financials
    27	NYSE Healthcare
    28	NYSE TMT
    29	S&P 500 Real Estate
    30	标普500指数公用事业板块
    31	标普500指数医疗保健板块
    32	标普500指数原材料板块
    33	标普500指数工业板块
    34	标普500指数必需消费品板块
    35	标普500指数科技板块
    36	标普500指数能源板块
    37	标普500指数通信服务板块
    38	标普500指数金融板块
    39	标普500指数非必需消费品板块
    40	费城半导体指数
    '''
    # us-main index 
    us_main_index =[ '纳斯达克100',
                     '纳斯达克综合指数',
                     '标普500指数',
                     '道琼斯指数',
                     'DJ Basic Materials',
                     'DJ Consumer Goods',
                     'DJ Consumer Services',
                     'DJ Financials',
                     'DJ Health Care',
                     'DJ Industrials',
                     'DJ Oil & Gas',
                     'DJ Technology',
                     'DJ Telecommunications',
                     'DJ Utilities',
                     'NASDAQ Bank',
                     'NASDAQ Biotechnology',
                     'NASDAQ Computer',
                     'NASDAQ Financial 100',
                     'NASDAQ Health Care',
                     'NASDAQ Industrial',
                     'NASDAQ Insurance',
                     'NASDAQ Internet',
                     'NASDAQ Other Finance',
                     'NASDAQ Telecommunications',
                     'NASDAQ Transportation',
                     'NYSE Energy',
                     'NYSE Financials',
                     'NYSE Healthcare',
                     'NYSE TMT',
                     'S&P 500 Real Estate',
                     '标普500指数公用事业板块',
                     '标普500指数医疗保健板块',
                     '标普500指数原材料板块',
                     '标普500指数工业板块',
                     '标普500指数必需消费品板块',
                     '标普500指数科技板块',
                     '标普500指数能源板块',
                     '标普500指数通信服务板块',
                     '标普500指数金融板块',
                     '标普500指数非必需消费品板块',
                     '费城半导体指数']  
    return pd.DataFrame({'index_code': us_main_index})

        
def code_name_a_stock() -> pd.DataFrame:
    """
    code and name list table for China's stock A market

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

def a_stock_codeset():
    '''
    '''
    stock_a = code_name_a_stock()
    return set(stock_a['stock_code'])

def sz_stock_codeset():
    '''
    

    Returns
    -------
    None.

    '''
    stock_sz = ak.stock_info_sz_name_code(indicator="A股列表")
    
    return set(stock_sz['A股代码'])

def sh_stock_codeset():
    '''
    

    Returns
    -------
    None.

    '''
    stock_sh = ak.stock_info_sh_name_code(indicator="主板A股")    
    return set(stock_sh['SECURITY_CODE_A'])


def a_stock_index_codeset():
    '''
    
    '''
    index_list = code_name_a_index()
    
    return set(index_list['index_code'])

def us_main_index_codeset():
    '''
    '''
    index_list = code_name_us_main_index()['index_code']
    return set(index_list) 

def em_fund_codeset():
    '''
    '''
    em_code = code_name_em_fund()
    return set(em_code['fund_code'])    

if __name__ == '__main__':
    
    stock_a_list = [
        "002250",
        "000525",
        "000553",
        "600731",
        "600389",
        "600596",
        "600486",
        "002004",
        "002215",
        "002258",
        "002391",
        "300261",
        "603599",
        "300575",
        "002734",
        "002749",
        "603585",
        "603086",
        "603639",
        "603360",
        "603810",
        "002942",
        "300796",
        ]
    
    def fun():
        return stock_a_list
    
    data = _iterate_for_code(stock_a_list, 
                      ak.stock_zh_a_daily, 
                      1,
                      max_try=10, 
                      adjust='qfq')
    # data.to_excel('stock_price.xlsx')

