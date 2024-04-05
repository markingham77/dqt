import sys
from pydqt.pydqt import Query, get_global_template_dir, create_test_data, setup_env
from pydqt.pydqt import test_data_exists as does_test_data_exist
from pydqt.pydqt import test_data_file_full_path as full_path_test_data_file
from pydqt.pydqt import get_user_template_dir, get_user_includes_dir, get_user_macros_dir
import pydqt.pydqt as dqt
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


def set_temp_workspace(root='/tmp',name='pydqt_env_delme'):
    if os.path.exists(f'{root}/{name}'):
        shutil.rmtree(f'{root}/{name}')
    dqt.set_workspace(root='/tmp',name='pydqt_env_delme')


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

def test_write_sql_unique_on_create():
    """
    tests dqt's writing to sql if unique is specified.  This should prevents any duplicates of a particular column
    """    
    query = Query(query="select * from '{{table}}' limit 10;",table=full_path_test_data_file())
    query.load()
    query.write_sql("dqt_delme_test",schema="CORE_WIP", append=False, unique='SOURCE')
    query_check = Query(query='select * from dqt_delme_test')
    query_check.run(schema='core_wip', database='lyst')
    tf = query_check.df.duplicated(subset=['SOURCE'])
    assert tf.any()==False


def test_write_sql_unique_on_append():
    """
    tests dqt's writing to sql if unique is specified.  This should prevents any duplicates of a particular column
    """    
    query = Query(query="select * from '{{table}}' limit 100;",table=full_path_test_data_file())
    query.load()
    df_no_dups = query.df.drop_duplicates(subset=['source'])
    df_to_append = pd.DataFrame(data=np.array([[pd.to_datetime('2022-01-31'),94,584.37,'US','css'],[pd.to_datetime('2022-03-30'),103,523.10,'NZ','new_source']]),columns=['dates','orders','gmv','region','source'])
    query.df = pd.concat([df_no_dups, df_to_append]).reset_index(drop=True)
    query.write_sql("dqt_delme_test",schema="CORE_WIP", database='lyst', append=True, unique='source')
    query_check = Query(query='select * from dqt_delme_test')
    query_check.run(schema='core_wip', database='lyst')
    tf = query_check.df.duplicated(subset=['SOURCE'])
    assert tf.any()==False

def test_write_sql_create_replace_date():
    """
    tests dqt's writing to sql
    """    
    query = Query(query="select * from '{{table}}' limit 10;",table=full_path_test_data_file())
    query.load()
    query.write_sql("dqt_delme_test",schema="CORE_WIP", append=False, timestamp=False)

def test_write_sql_create_replace():
    """
    tests dqt's writing to sql
    """    
    query = Query(query="select * from '{{table}}' limit 10;",table=full_path_test_data_file())
    query.load()
    query.write_sql("dqt_delme_test",schema="CORE_WIP", append=False)

def test_write_sql_append():
    """
    tests dqt's writing to sql
    """    
    query = Query(query="select * from '{{table}}' limit 10;",table=full_path_test_data_file())
    query.load()
    query.write_sql("dqt_delme_test",schema="CORE_WIP", append=True)

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

def test_set_workspace_exists_for_absolute_root():
    """
    tests set_workspace function wihtin pydqt
    """    
    root='/tmp'
    name='pydqt_env_delme'
    set_temp_workspace(root=root, name=name)
    assert os.path.exists(f'{root}/{name}')

# def test_set_workspace_exists_for_relative_root():
#     """
#     tests set_workspace function with root that does not start with "/"

#     Note: it is ultimately get_ws that deals with this by returning root
#     """    
#     root='research'
#     name='pydqt_env_delme'
#     set_temp_workspace(root=root, name=name)
#     assert os.path.join(Path(__file__).parents[1], f'{root}/{name}') == '/'.join(dqt.get_ws())

def test_set_workspace_has_cache():
    """
    tests set_workspace function wihtin pydqt
    """    
    root='/tmp'
    name='pydqt_env_delme'
    set_temp_workspace(root=root, name=name)

    assert os.path.exists(f'{root}/{name}/cache')

def test_set_workspace_has_templates():
    """
    tests set_workspace function wihtin pydqt
    """    
    root='/tmp'
    name='pydqt_env_delme'
    set_temp_workspace(root=root, name=name)
    assert os.path.exists(f'{root}/{name}/templates')

def test_set_workspace_has_includes():
    """
    tests set_workspace function wihtin pydqt
    """    
    root='/tmp'
    name='pydqt_env_delme'
    set_temp_workspace(root=root, name=name)

    assert os.path.exists(f'{root}/{name}/templates/includes')

def test_set_workspace_has_compiled():
    """
    tests set_workspace function wihtin pydqt
    """    
    root='/tmp'
    name='pydqt_env_delme'
    set_temp_workspace(root=root, name=name)
    assert os.path.exists(f'{root}/{name}/templates/compiled')

def test_set_workspace_has_macros():
    """
    tests set_workspace function wihtin pydqt
    """    
    root='/tmp'
    name='pydqt_env_delme'
    set_temp_workspace(root=root, name=name)
    assert os.path.exists(f'{root}/{name}/templates/macros/mymacros.jinja')

def test_change_of_workspace():
    """
    change workspace via set_workspace and then confirm that compile points to the correct dirs
    """
    root='/tmp'
    name='pydqt_env_delme'
    set_temp_workspace(root=root, name=name)
    user_dir = get_user_template_dir()
    # print(user_dir)
    assert user_dir==f'{root}/{name}/templates'

