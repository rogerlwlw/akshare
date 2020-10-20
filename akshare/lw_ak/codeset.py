# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 16:02:17 2020

@author: roger luo
"""

import pandas as pd
import akshare as ak

from akshare.lw_ak.code_asset_attr import a_stock_attr, em_fund_attr

def stock_a_code():
    """
    

    Returns
    -------
    China's A stock market  code set

    """
    stock_a = a_stock_attr()
    return set(stock_a['stock_code'])
    

def index_a_code():
    '''
    '''
    
    data = ak.index_stock_info() 
    
    return set(data['index_code'])

def em_fund_code():
    '''
    '''
    em_code = em_fund_attr()
    return set(em_code['fund_code'])   

def index_us_code():
    '''
     return main US index code set
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
    return set(us_main_index)

