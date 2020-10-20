# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 16:23:22 2020

@author: Administrator
"""


import akshare as ak

import random
import pandas as pd

from utilis import _iterate_for_code, merge_dfs


def a_market_pe(max_try: int =50, test_num: int =-1):
    '''
    获取乐咕乐股网站-A股市盈率
    
    '''
    
    data = _iterate_for_code(pe_pb_codeset, 
                                   ak.stock_a_pe, 
                                   test_num,
                                   max_try=max_try, 
                                   )   
    data['code'] = data['code'].map(
        {v : k for k, v in pe_code.items()})    
    return data

def a_market_pb(max_try: int =50, test_num: int =-1):
    '''
    
    获取乐咕乐股网站-A 股市净率
    
    '''

    data = _iterate_for_code(pe_pb_codeset, 
                            ak.stock_a_pb, 
                            test_num,
                            max_try=max_try, 
                                   )
    
    data['code'] = data['code'].map(
        {v : k for k, v in pe_code.items()})
    return data

def pe_pb_codeset():
    '''
    '''
    
    return set(pe_code.values())

pe_code = {
        "上证A股市盈率": "sh",
        "深圳A股市盈率": "sz",
        "中小板市盈率": "zx",
        "创业板市盈率": "cy",
        "科创板市盈率": "kc",
        "全部A股市盈率-平均数-中位数": "all",
        "沪深300市盈率": "000300.XSHG",
        "上证50市盈率": "000016.XSHG",
        "上证180市盈率": "000010.XSHG",
        "上证380市盈率": "000009.XSHG",
        "中证流通市盈率": "000902.XSHG",
        "中证100市盈率": "000903.XSHG",
        "中证500市盈率": "000905.XSHG",
        "中证800市盈率": "000906.XSHG",
        "中证1000市盈率": "000852.XSHG",
        }
# 宏观环境
#%% 股票质押数据 检测平仓线 市场质押率

#%% 沪深港通 持股数 北向资金

#%% 股票账户统计
# 描述: 获取东方财富网-数据中心-特色数据-股票账户统计

