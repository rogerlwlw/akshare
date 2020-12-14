# -*- coding: utf-8 -*-
"""
Created on Wed Oct 21 16:15:52 2020

@author: roger luo
"""

from akshare.lw_ak.asset_daily import asset_pool
from akshare.lw_ak.utilis import iterate_for_code

from functools import wraps
from lwmlearn.utilis.dbconn import EngineConn


def _timestamp(s):
    '''
    '''
    try:
        from datetime import  datetime
        now = datetime.now()
        return s.format(now.strftime("%Y_%m"))
    except:
        return s

@wraps(iterate_for_code)
def down_asset_hist(asset_list=-1,
                    test_num=-1, max_try=10, 
                    url="sqlite:///capmkt{}.db?check_same_thread=False", 
                    **kwargs):
    """down load asset historical price data
    
    parameters
    -----------
    assert_list : int or list
        if -1, download all aseets in asset_pool
        
        if 0, print available asset keys
        
        if list, download listed assets

    """
    global ENGINE, DB_URL
    
    if url != DB_URL:
        DB_URL = _timestamp(url)
        ENGINE = EngineConn(DB_URL)
    
    if asset_list == -1:
        assets = asset_pool(-1)
    elif asset_list == 0 :
        print('available assets are: %s'% asset_pool(-1).keys())
        return
    else:
        assets = {k : v for k, v in asset_pool(-1).items() 
                  if k in asset_list}
    
    for k, v in assets.items():
        data = iterate_for_code(*v, test_num=test_num, max_try=max_try,
                                **kwargs)
        ENGINE.upload(data, k, if_exists='replace')

DB_URL = _timestamp("sqlite:///capmkt{}.db?check_same_thread=False")

ENGINE = EngineConn(DB_URL)
        
if __name__ == "__main__":
    pass
    down_asset_hist(-1)
         