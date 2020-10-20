# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 17:29:35 2020

@author: Administrator
"""

import akshare as ak

import random
import pandas as pd

from daily_data import (a_stock_codeset,
                        a_stock_index_codeset,
                        sh_stock_codeset,
                        sz_stock_codeset,
                        )
from utilis import _iterate_for_code, merge_dfs


def make_quarter_str(dt):
    '''
    return
    ----
    quarter indicator str for a given date, '2010-02-01' -> '20101' means first
    quarter.
    '''
    year = dt.year
    month = dt.month
    quarter = (month - 1) // 3 + 1
    
    return ''.join([str(year), str(quarter)])


def stock_cashlow100(max_try: int =50, test_num: int =-1):
    '''
    描述: 获取东方财富网-数据中心-个股资金流向

    限量: 单次获取指定市场和股票的近 100 个交易日的资金流数据
    '''
    sh_stock_data = _iterate_for_code(sh_stock_codeset, 
                                   ak.stock_individual_fund_flow, 
                                   test_num,
                                   max_try=max_try, 
                                   market='sh')   
    
    sz_stock_data = _iterate_for_code(sz_stock_codeset, 
                                   ak.stock_individual_fund_flow, 
                                   test_num,
                                   max_try=max_try, 
                                   market='sz')    
    
    return pd.concat([sh_stock_data, sz_stock_data], axis=0, ignore_index=True)

def stock_index_comp(max_try: int =50, test_num: int =-1):
    '''
    component of stock index
    return
    ----
    dataframe for component stock of an a stock index
    '''
    index_comp = _iterate_for_code(
        a_stock_index_codeset, ak.index_stock_cons, test_num, max_try=max_try)      
    
    return index_comp


def stock_institute_detail(quarter, max_try: int =50, test_num: int =-1):
    '''
    机构持股 前十大流通股东 明细
    
    截至具体季度 quarter
    
    '''
    quarter = make_quarter_str(pd.to_datetime(quarter))
    institute_hold_detail = _iterate_for_code(
        a_stock_codeset, ak.stock_institute_hold_detail, test_num, 
        max_try=max_try, quarter=quarter)      
    
    return institute_hold_detail

def stock_institute_hold(quarter):
    '''
    机构持股 前十大流通股东 汇总
    截至具体季度 quarter

    Parameters
    ----------
    quarter : datetime string.
        financial report date.

    Returns
    -------
    None.

    '''
    quarter = make_quarter_str(pd.to_datetime(quarter))
    return ak.stock_institute_hold(quarter=quarter)

def stock_rating():
    '''
    机构评级的 统计
    
    标签类型选择:
        {'最新投资评级', '上调评级股票', '下调评级股票', '股票综合评级', 
        '首次评级股票', '目标涨幅排名', '机构关注度', '行业关注度',
        '投资评级选股'}
    '''
    indicator = {'股票综合评级', '目标涨幅排名', '机构关注度',
                 '投资评级选股'}
    df_list = []        
    for i in indicator:
        df_list.append(ak.stock_institute_recommend(i))
        
    return merge_dfs(df_list, 'outer')



def stock_rating_hist(max_try: int =50, test_num: int =-1):
    '''
    return
    -----
        ak.stock_institute_recommend_detail for all stock code
    '''

    institute_rating_data = _iterate_for_code(
        a_stock_codeset, ak.stock_institute_recommend_detail, test_num, 
        max_try=max_try)  
    
    return institute_rating_data


def stock_sina_fundhold_hist(max_try: int =50, test_num: int =-1):
    '''
    描述: 获取新浪财经-股本股东-基金持股

    限量: 单次获取新浪财经-股本股东-基金持股所有历史数据
    '''
    data = _iterate_for_code(
        a_stock_codeset, ak.stock_fund_stock_holder, test_num, 
        max_try=max_try)      
    
    return data
    
def em_analyst_index(max_try: int =50, test_num: int =-1):
    '''
    east money analyst index
    '''
    # df = ak.stock_em_analyst_detail(indicator='历史指数')
    analyst_data = _iterate_for_code(em_analyst_codeset, 
                                     ak.stock_em_analyst_detail,
                                     test_num,
                                     max_try,
                                     indicator='历史指数')
    return analyst_data

def em_analyst_codeset():
    '''
    '''
    
    df_data = ak.stock_em_analyst_rank()
    
    return set(df_data.FxsCode)



