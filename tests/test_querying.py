from pydqt.pydqt import Query, get_global_template_dir, get_example_template_names, create_test_data, test_data_exists, test_data_file_full_path
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



# def test_test_data():
#     TEST_DATA = os.path.join(Path(__file__).parents[1],'test.csv')
#     if not os.path.exists(TEST_DATA):
#         create_test_data()
#         assert os.path.exists(TEST_DATA), "cannot create test data"

# def get_test_data_file():
#     return os.path.join(Path(__file__).parents[0],'pydqt/test.csv')

def get_test_data():
    if test_data_exists():
        TEST_DATA = test_data_file_full_path()
        return pd.read_csv(TEST_DATA)
    else:
        df = create_test_data()
        return df

    if not os.path.exists(TEST_DATA):
        create_test_data()
        assert os.path.exists(TEST_DATA), "cannot create test data"
    

def test_sql_command():
    """
    test a sql command
    """
    if not test_data_exists():
        create_test_data()
    query = Query(query="select * from '{{table}}' limit 10;",table=get_test_data_file())

    query.load()
    assert len(query.df)>0  

def test_macro():
    """
    test a sql command
    """
    if not test_data_exists():
        create_test_data()
    query = Query(query="""
                select *,  
                {{macros.ma('orders',4,order='dates',partition='region,source')}} ma_orders, 
                {{macros.ma('gmv',4,order='dates',partition='region,source')}} ma_gmv,   
                from '{{table}}' 
                limit 10;
            """,table=test_data_file_full_path())

    query.load()
    # print(query.df.head())
    assert len(query.df)>0  

# def test_template():
#     q=Query('test.sql', min_query_date='2023-01-01', table=test_data_file_full_path())
#     q.load()
#     assert len(q.df)>0
