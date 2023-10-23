import sys
from pydqt import Query, get_global_template_dir, create_test_data
from pydqt import test_data_exists as does_test_data_exist
from pydqt import test_data_file_full_path as full_path_test_data_file
import os

import shutil
from pathlib import Path
import pandas as pd
import sqlparse
import pytest
import numpy as np

########################################################################################################################
####  some set up - create temp template with {{table}} variable, where table = 'lyst_analytics.union_touch_points' ####
########################################################################################################################




def get_test_data():
    if test_data_exists():
        TEST_DATA = test_data_file_full_path()
        return pd.read_csv(TEST_DATA)
    else:
        df = create_test_data()
        return df
    

def test_sql_command():
    """
    test a sql command
    """
    # if not test_data_exists():
    #     create_test_data()
    query = Query(query="select * from '{{table}}' limit 10;",table=full_path_test_data_file())

    query.load()
    assert len(query.df)>0  

def test_macro():
    """
    test a sql command
    """
    if not does_test_data_exist():
        create_test_data()
    query = Query(query="""
                select *,  
                {{macros.ma('orders',4,order='dates',partition='region,source')}} ma_orders, 
                {{macros.ma('gmv',4,order='dates',partition='region,source')}} ma_gmv,   
                from '{{table}}' 
                limit 10;
            """,table=full_path_test_data_file())

    query.load()
    # print(query.df.head())
    assert len(query.df)>0  

# def test_template():
#     q=Query('test.sql', min_query_date='2023-01-01', table=test_data_file_full_path())
#     q.load()
#     assert len(q.df)>0
