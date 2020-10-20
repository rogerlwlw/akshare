# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 14:18:40 2020

@author: Administrator
"""

import logging
import os

class INFO_Filter(logging.Filter):
    '''
    reconstruct filter method to return bool value to filter logrecord,
    
    Logrecord has attributes like below that could be used as filters:
        [name, levelname]
    
    '''
    def filter(self, record):
        if record.levelname == "INFO":
            return True
        else:
            return False
    
LOG_FMT = "loger: %(name)s - %(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"


def init_log(loger_name='download', error_log='log/akshare_error.log',
             info_log='log/akshare_info.log', file_mode='w'):
    """
    output error log to 'error_log' file, info log to 'info_log file',
    and all log above info to sys.stdout 

    Parameters
    ----------
    loger_name : TYPE, optional
        DESCRIPTION. The default is 'download'.
    error_log : TYPE, optional
        DESCRIPTION. The default is 'akshare_error.log'.
    info_log : TYPE, optional
        DESCRIPTION. The default is 'akshare_info.log'.
    file_mode : TYPE, optional
        DESCRIPTION. The default is 'w'.

    Returns
    -------
    loger : TYPE
        DESCRIPTION.

    """
    
    # get loger instance by loger_name, if not exist, create one
    loger = logging.getLogger(loger_name)
    loger.setLevel(logging.DEBUG)

    
    if not loger.handlers:
        # make dirs for log files
        for item in [info_log, error_log]:
            os.makedirs(os.path.split(item)[0], exist_ok=True) 
        # error handler to capture error and above error info 
        e_h = logging.FileHandler(error_log, file_mode)
        e_h.setLevel(logging.ERROR)
        e_h.setFormatter(logging.Formatter(LOG_FMT, datefmt=DATE_FORMAT))
        loger.addHandler(e_h)
        
        # info handler only
        info_h = logging.FileHandler(info_log, file_mode)
        info_h.setLevel(logging.INFO)
        info_h.setFormatter(logging.Formatter(LOG_FMT, datefmt=DATE_FORMAT))
        info_h.addFilter(INFO_Filter())
        loger.addHandler(info_h)
        
        # streamhandler output all record to sys.stdout
        s_h = logging.StreamHandler()
        s_h.setLevel(logging.INFO)
        loger.addHandler(s_h)
        
    return loger

