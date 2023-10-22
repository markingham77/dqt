from dqt.dqt import Query, get_global_template_dir, get_example_template_names
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

def create_sql_table():
    """
    creates a sql table
    """
    # create a random matric and write to csv
    def create_random_df(nrows, ncols):
        m = np.random


def test_sql_command():
    """
    test a sql command
    """
    query = Query(query="select * from test limit 10;")
    query.load()
    assert len(query.df)>0  

