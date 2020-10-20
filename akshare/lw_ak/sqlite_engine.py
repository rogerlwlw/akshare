# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 16:47:32 2020

@author: Administrator
"""

import sqlalchemy

def get_sqlite_engine(db='capital_market'):
    '''
    

    Parameters
    ----------
    db : TYPE, optional
        DESCRIPTION. The default is 'sqlite:///test.db?check_same_thread=False'.

    Returns
    -------
    None.

    '''
    #--sqlite默认建立的对象只能让建立该对象的线程使用，
    #而sqlalchemy是多线程的所以我们需要指定check_same_thread=False
    #来让建立的对象任意线程都可使用。否则不时就会报错：
    #sqlalchemy.exc.ProgrammingError: 
    db_conn = 'sqlite:///{}.db?check_same_thread=False'.format(db)
    return sqlalchemy.create_engine(db_conn, echo=False)
