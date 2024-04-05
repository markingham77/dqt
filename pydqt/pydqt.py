import math, random
import sqlparse
import snowflake.connector
import pathlib
import datetime
import re
import json
from jinja2 import Environment, FileSystemLoader
import os
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import numpy as np
from pprint import pprint
import uuid
import duckdb
from duckdb import CatalogException
from pathlib import Path
import shutil
from snowflake.connector.pandas_tools import write_pandas
from .utils import custom_filters as filters


class NoDataException(Exception):
    pass

def env_file_full_path():
    return os.path.join(Path(__file__).parents[0],'.env')

def test_data_file_full_path(dups=True):
    return os.path.join(Path(__file__).parents[0],'test.csv')

def test_data_exists():
    test_data_file = test_data_file_full_path()
    if os.path.exists(test_data_file):
        return True
    return False

def create_test_data():
    """
    creates a test.csv data file which we can run tests against and users can practice with
    """
    regions = ['US','GB','DE','IT','ES','FR']
    sources = ['css','direct','organic','app']
    dates = pd.date_range(start='2022-01-01',periods=21,freq='M')

    cnt=0
    for region in regions:
        for source in sources:
            orders = [math.floor(100*random.uniform(0,1)) for i in dates] 
            gmv = [100*x*random.uniform(0,1) for x in orders]
            df_new = pd.DataFrame({
                'dates': dates,
                'orders': orders,
                'gmv': gmv,
                'region': region,
                'source': source
            })
            if cnt==0:
                df = df_new
            else:
                df=pd.concat([df,df_new])
            cnt+=1  
    outfile = test_data_file_full_path()
    df = df.round({'orders': 0, 'gmv': 2})    
    df['orders'] = df['orders'].astype(int) 
    df.to_csv(outfile, index=False)    
    
                      
    return df


def set_snowflake_credentials(login='',role=''):
    """
    sets snowflake credentials; login (usually an email) and role (which scopes out privileges)
    """
    assert login!='', 'you must specify your login'
    assert role!='', 'you must specify your role'
    env_file = env_file_full_path()
    new_env_file = 'new.env'

    with open(env_file, 'r') as r, open(new_env_file, 'w') as w:
        for line in r: 
            if line.strip().startswith('SNOWFLAKE_LOGIN'): 
                w.write(f'SNOWFLAKE_LOGIN = \'{login}\'\n')
            elif line.strip().startswith('SNOWFLAKE_ROLE'): 
                w.write(f'SNOWFLAKE_ROLE = \'{role}\'\n')
            elif line.strip():
                w.write(line + '\n')
    shutil.move(new_env_file, env_file)

    # now reload env and setup_local_dirs
    env_reload()
    return

def set_schema(schema):
    """
    ammends the CURRENT_SCHEMA property of DB_SETTINGS object
    """
    assert schema, "you didn't specify a schema"
    DB_SETTINGS['CURRENT_SCHEMA']=schema.upper()

def get_schema():
    """
    retrieves the CURRENT_SCHEMA property of DB_SETTINGS object
    """    
    return DB_SETTINGS['CURRENT_SCHEMA']

def set_database(db):
    """
    ammends the CURRENT_SCHEMA property of DB_SETTINGS object
    """
    assert db, "you didn't specify a schema"
    DB_SETTINGS['CURRENT_DATABASE']=db.upper()

def get_database():
    """
    retrieves the CURRENT_SCHEMA property of DB_SETTINGS object
    """    
    return DB_SETTINGS['CURRENT_DATABASE']

def set_warehouse(wh):
    """
    ammends the CURRENT_SCHEMA property of DB_SETTINGS object
    """
    assert wh, "you didn't specify a schema"
    DB_SETTINGS['CURRENT_WAREHOUSE']=wh.upper()

def get_warehouse():
    """
    retrieves the CURRENT_SCHEMA property of DB_SETTINGS object
    """    
    return DB_SETTINGS['CURRENT_WAREHOUSE']

def get_db_settings():
    print('Current DB Settings are:')
    print(DB_SETTINGS)
    print('You can run: set_warehouse, set_database, set_schema to change any of these')
    return DB_SETTINGS

def set_workspace(root='',name=''):    
    """
    sets the workspace environment .env file and environment variables using specified root and name params.
    If root is not specified then this uses the current workspace's root.
    """
    
    
    if root=='':
        root, _ = get_ws()
        # root = os.path.join(Path(__file__).parents[0],'workspaces')
    if name=='':
        name = 'main'    

    # first rewrite .env file to reflect the root and name
    env_file = env_file_full_path()
    new_env_file = 'new.env'

    with open(env_file, 'r') as r, open(new_env_file, 'w') as w:
        for line in r: 
            if line.strip().startswith('WORKSPACE_ROOT'): 
                w.write(f'WORKSPACE_ROOT = \'{root}\'\n')
            elif line.strip().startswith('WORKSPACE_NAME'): 
                w.write(f'WORKSPACE_NAME = \'{name}\'\n')
            elif line.strip():
                w.write(line + '\n')
    shutil.move(new_env_file, env_file)

    # now reload env and setup_local_dirs
    USER_DIR = env_reload()

    return

def env_reload():
    load_dotenv(
        # Path(find_dotenv(usecwd=True)),
        # Path('.env'),
        env_file_full_path(),
        override=True
    ) 
    return setup_local_dirs()

def env_edit():
    # filename = os.path.join(Path(__file__).parents[0],'.env')
    filename = env_file_full_path()
    os.system(f'open {filename}') 

    load_dotenv(
        # Path(find_dotenv(usecwd=True)),
        Path(filename),                    
        override=True
    ) 
    setup_local_dirs()

def get_ws():
    """
    get workspace root and name
    """    
    if "WORKSPACE_ROOT" not in os.environ:
        ws_dir = os.path.join(Path(__file__).parents[0],'workspaces')
    else:
        ws_dir = os.environ['WORKSPACE_ROOT']    
        if len(ws_dir)==0:
            ws_dir = os.path.join(Path(__file__).parents[0],'workspaces')
        else:
            if ws_dir[0]!='/':
                ws_dir = os.path.join(Path(__file__).parents[0],ws_dir) 
    if "WORKSPACE_NAME" not in os.environ:
        name = 'main'
    else:        
        name = os.getenv("WORKSPACE_NAME")    
        if len(name)==0:
            name = 'main'

    return (ws_dir, name)                

def get_workspace():
    return get_ws()

def setup_local_dirs():
    load_dotenv(
            # Path('.env'),                        
            env_file_full_path(),
        )  # find .env automagically by walking up directories until it's found

    ws_dir, name = get_ws()
    if not os.path.exists(ws_dir):
        os.mkdir(ws_dir)
    user_dir = os.path.join(ws_dir,name)
    if not os.path.exists(user_dir):
        os.mkdir(user_dir)

    template_dir = os.path.join(user_dir,'tests')
    if not os.path.exists(template_dir):
        os.mkdir(template_dir)
    for subdir in ['sql','json',]:
        if not os.path.exists(os.path.join(template_dir,subdir)):
            os.mkdir(os.path.join(template_dir,subdir))      

    template_dir = os.path.join(user_dir,'templates')
    if not os.path.exists(template_dir):
        os.mkdir(template_dir)
    for subdir in ['compiled','includes','macros']:
        if not os.path.exists(os.path.join(template_dir,subdir)):
            os.mkdir(os.path.join(template_dir,subdir))                                           
    if not os.path.exists(os.path.join(template_dir,'macros/mymacros.jinja')):
        with open(os.path.join(template_dir,'macros/mymacros.jinja'),'w') as f:
            pass        
    
    cache_dir = os.path.join(user_dir,'cache')
    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)
    db_cache_dir = os.path.join(cache_dir,'snowflake')
    if not os.path.exists(db_cache_dir):
        os.mkdir(db_cache_dir)

    return user_dir

def setup_env():
    filename = env_file_full_path()
    if not os.path.exists(filename):
        print('.env does not exist - creating unfilled .env file')
        with open(filename,'w') as f:
            f.write("""SNOWFLAKE_LOGIN = ''
SNOWFLAKE_ROLE = ''
SNOWFLAKE_DEFAULT_DATABASE = ''
SNOWFLAKE_DEFAULT_SCHEMA = ''
WORKSPACE_ROOT = ''
WORKSPACE_NAME = ''""")
    else:
        found_login = False
        found_role = False
        found_db = False
        found_schema = False
        with open(filename,'r') as f:        
            for line in f:
                if line.startswith("SNOWFLAKE_DEFAULT_DATABASE"):
                    found_db = True
                elif line.startswith("SNOWFLAKE_DEFAULT_SCHEMA"):
                    found_schema = True
                elif line.startswith("SNOWFLAKE_LOGIN"):
                    found_login = True
                elif line.startswith("SNOWFLAKE_ROLE"):
                    found_role = True
        if not found_db:
            with open(filename, 'a') as f:
                f.write("\nSNOWFLAKE_DEFAULT_DATABASE = ''")
        if not found_schema:
            with open(filename, 'a') as f:
                f.write("\nSNOWFLAKE_DEFAULT_SCHEMA = ''")
        if not found_login:
            with open(filename, 'a') as f:
                f.write("\nSNOWFLAKE_LOGIN = ''")
        if not found_role:
            with open(filename, 'a') as f:
                f.write("\nSNOWFLAKE_ROLE = ''")
                


# Initial setup when pydqt is imported
# user_dir = setup_local_dirs()
setup_env()
USER_DIR = env_reload()
DB_SETTINGS={}
DB_SETTINGS['CURRENT_SCHEMA'] = os.getenv("SNOWFLAKE_DEFAULT_SCHEMA")
DB_SETTINGS['CURRENT_DATABASE'] = os.getenv("SNOWFLAKE_DEFAULT_DATABASE")
role = os.getenv("SNOWFLAKE_ROLE")
DB_SETTINGS['CURRENT_WAREHOUSE'] = f"{role}_QUERY_LARGE_WH"


if not test_data_exists():
    create_test_data()

def py_connect_db(warehouse = get_warehouse(), database=get_database(), schema=get_schema()) -> snowflake.connector.connection.SnowflakeConnection:
    """connect to snowflake, ensure SNOWFLAKE_LOGIN defined in .env"""


    load_dotenv(
        env_file_full_path()
        # Path('.env'),
    )  # find .env automagically by walking up directories until it's found

    def check_env_var(var):
        if var not in os.environ:
            if var == "SNOWFLAKE_LOGIN":
                raise EnvironmentError(
                    f"Failed. Please set {var}=<lyst email> & SNOWFLAKE_ROLE in your .env file"
                )
            else:
                raise EnvironmentError(
                    f"Failed. Please set {var} in your .env file - via env_edit()"
                )
        else:    
            if os.environ[var]=='':
                if var == "SNOWFLAKE_LOGIN":
                    raise EnvironmentError(
                        f"Failed. Please set {var}=<lyst email> & SNOWFLAKE_ROLE in your .env file"
                    )
                else:
                    raise EnvironmentError(
                        f"Failed. Please set {var} in your .env file - via env_edit()"
                    )

    for var in ["SNOWFLAKE_LOGIN", "SNOWFLAKE_ROLE", "SNOWFLAKE_DEFAULT_DATABASE", "SNOWFLAKE_DEFAULT_SCHEMA"]:
        check_env_var(var)
    # if "SNOWFLAKE_LOGIN" not in os.environ:
    #     raise EnvironmentError(
    #         "Failed. Please set SNOWFLAKE_LOGIN=<lyst email> & SNOWFLAKE_ROLE in your .env file"
    #     )
    # if os.environ["SNOWFLAKE_LOGIN"]=='':
    #     raise EnvironmentError(
    #         "Failed. Please set SNOWFLAKE_LOGIN=<lyst email> & SNOWFLAKE_ROLE in your .env file"
    #     )

    # Now have default values in function definition, above
    SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
    # if not warehouse:
    #     warehouse=f"{SNOWFLAKE_ROLE}_QUERY_LARGE_WH"
    # if not database:
    #     database = os.getenv("SNOWFLAKE_DEFAULT_DATABASE")
    # if not schema:
    #     schema = os.getenv("SNOWFLAKE_DEFAULT_SCHEMA")

    return snowflake.connector.connect(
        account="fs67922.eu-west-1",
        user=os.getenv("SNOWFLAKE_LOGIN"),
        authenticator="externalbrowser",
        database=database,
        schema=schema,
        role=SNOWFLAKE_ROLE,
        warehouse=warehouse,
        client_session_keep_alive=True,
    );


def files_are_equal(f1,f2) -> bool:
    """
    compares two text files to see if they are the same
    """
    if os.path.getsize(f1) == os.path.getsize(f2):
        if open(f1,'r').read() == open(f2,'r').read():
            return True
        else:
            return False
    else:
        return False    
    
def describe_df(df):
    return f'{df.shape[0]} rows, {df.shape[1]} cols'

def cache_dir(template: str='', **kwargs):
    """
    returns dir loc of cache data (a folder containing .csv data and a .sql fileof compiled sql)
    """
    fn=template.lower().replace('.sql','')

    if len(kwargs.items())>0:
        for i, (key, val) in enumerate(kwargs.items()):
            if type(val)==list:
                fn = fn + '__' + key + '__' + "|".join(val)
            elif (type(val)==int) | (type(val)==float): 
                fn = fn + '__' + key + '__' + str(val)
            else:
                fn = fn + '__' + key + '__' + val
    ws_root, ws_name = get_ws()
    return os.path.join(ws_root,ws_name,'cache/snowflake/',fn)
    # return os.path.join(USER_DIR,'cache/snowflake/',fn)
    # return os.path.join(str(Path(__file__).parents[1]),'user/cache/snowflake/',fn)

def temp_sql_compiled_template_dir():
    ws_root, ws_name = get_ws()
    return os.path.join(ws_root,ws_name,'templates/compiled')
    # return os.path.join(USER_DIR,'templates/compiled')
    # return os.path.join(str(Path(__file__).parents[1]),'user/templates/compiled')

class Workspace:
    """
    Workspace class which gets current workspace upon object instantiation.

    Workspace objects have two methods:
     
        - export: will export the workspace to a specified location
        - publish: will publish to a specified repo (usually a public repo so users can share workspaces)
    """
    def __init__(self):
        root, name = get_ws()
        self.root = root
        self.name = name
        self.full_path = os.path.join(root, name)

    def __repr__(self):
        return f'Workspace(root=\'{self.root}\', name=\'{self.name}\')'
    
    def export(self, to='', ow=False):
        assert to!='', f'You must specifiy a destination to export to (via \'to\' param)'
        if ow==False:
            assert not os.path.exists(to), f'your destination \'{to}\' already exists and ow=False so aborting.  If you want to export anyway, set ow=True.'
        if ow:
            if os.path.exists(to):
                shutil.rmtree(to)
        shutil.copytree(self.full_path,to)

        print(f'{self} was successfully exported to {to}')

    def publish(self, repo=''):
        print('publish method has not been implemented yet - todo!')


class QueryTemplateParams:
    """
    Template Params object which is is an attribute of Query class
    """
    def __init__(self, **kwargs):
        for (key, val) in kwargs.items():
            setattr(self,key,val)

class Sql:
    """
    Sql object which represents the compiled sql from Query.compile() method.
    To access the sql query as string, use Sql.text
    """

    def __init__(self, text='',temp_dir = temp_sql_compiled_template_dir()):
        self.text=sqlparse.format(text, reindent=True, keyword_case='upper')
        # handle ':LANGUAGE' not working
        self.text = self.text.replace(':LANGUAGE',':language')
        self.temp_dir = temp_dir
        self._remove_temp_sql_files()

    def __repr__(self):
        return(self.text)

    def _remove_temp_sql_files(self):
        for f in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, f))

    def open(self):
        self._remove_temp_sql_files()
        filename = str(uuid.uuid4())
        filename = os.path.join(self.temp_dir,f'{filename}.sql')
        with open(filename, 'w') as fobj:
            fobj.write(self.text)
        # print('OPENING')    
        os.system(f'open {filename}')    

class QueryParams:
    """
    QueryParams Class

    a class whose fields are the user-defined params in Query class
    """
    def __init__(self, disallowed=[],**kwargs):
        filtered_kwargs={k:v for k, v in kwargs.items() if k not in disallowed}
        for (key, val) in filtered_kwargs.items():
            setattr(self,key,val)

    def __repr__(self) -> str:
        s=''
        params={k:v for k, v in vars(self).items()}
        for i, (key, val) in enumerate(params.items()):
            s+=f"\n  {key}: {val}"
        return s
                


class Query:
    """
    Data Query Tool Class

    takes a sql command or template and specified params to run queries and cache results locally
    """
    def __init__(self, query='aggregate_user_data.sql', cache=True, **kwargs) -> None:

        self.query = query # query can be sql command string or template file name
        self.sql=None
        self.template=None
        self.cache = cache
        self.df = None
        self.tests = {}
        self.core_attributes = ['template','sql','cache','df','core_attributes','query','csv']
        self.params=QueryParams(disallowed=self.core_attributes,**kwargs)
        is_template,compiled_sql=compile(self.query, **self.params.__dict__)
        self.sql = Sql(text=compiled_sql)
        if is_template:
            self.template=self.query
            pattern = r'from\s+.*\.csv'
            matches = re.findall(pattern, self.sql.text, re.MULTILINE | re.IGNORECASE)
            if len(matches)>0:
                self.cache=False
        else:
            # don't cache adhoc sql queries
            self.cache=False    
        # self.__dict__.update(kwargs)
        if self.is_cached():
            self.csv=self.get_cache_files()[0]
        else:
            self.csv=None

    def is_cached(self):
        if self.cache:
            if (os.path.exists(self.get_cache_files()[0])):
                return True
            else:
                return False         
        else:
            if not self.template:
                return False
            else:
                return
    
    def get_cache_files(self):
        """
        returns the absolute cache csv file name and compiled sql filename for most recent param values on Query object

        returns a two-tuple, where first element is csv file and second is sql file.
        """
        if self.sql:
            dir_loc = cache_dir(self.template,**self.params.__dict__)
            return (os.path.join(dir_loc,'data.csv'),os.path.join(dir_loc,'data.sql'))

    def __repr__(self):
        df_desc = None
        if self.template:
            sql_str = f'sql from template (len: {len(self.sql.text)})'
        else:
            sql_str = f'sql from string (len: {len(self.sql.text)})'    
        if type(self.df) == pd.core.frame.DataFrame:
            df_desc = describe_df(self.df)
        s = f"""Class: Query (Data query tool)\n
sql: {sql_str}       
template: {self.template}
cache: {self.cache}
df: {df_desc}
is_cached: {self.is_cached()}
params: {self.params}"""

        return s

    def cache_is_synced(self):
        """
        return True if cached data's sql matches current sql, False otherwise
        """ 
        if self.is_cached():
            sql_file = self.get_cache_files()[1]
            if len(self.sql.text)>0:
                with open(sql_file, 'r') as fobj:
                    if fobj.read() == self.sql.text:
                        return True
                    else:
                        return False 
            else:
                return False
        else:
            return False    

    def load(self):
        """
        loads from cache if present, otherwise call .run()
        """
        if self.is_cached():
            cache_file=self.csv
            if self.cache_is_synced():
                # check data.sql matches current compiled sql - if it doesn't then rerun
                df = pd.read_csv(self.get_cache_files()[0], index_col=False)
                self.df=df     
            else:
                df = self.run()
                self.df=df

        else:
            df = self.run()
            self.df=df
        return self.df

    def run(self, database='', schema=''):
        # print(DB_SETTINGS)
        if database=='':
            database=get_database()

        if schema=='':
            schema=get_schema()

        # print('schema is', schema)
        if '.csv' in self.sql.text:
            df = duckdb.sql(self.sql.text).df()
        else:
            if schema!="":
                if database!="":
                    conn = py_connect_db(database=database,schema=schema)
                else:
                    conn = py_connect_db(schema=schema)
            else:
                if database!="":
                    conn = py_connect_db(database=database)
                else:               
                    conn = py_connect_db()
            df = pd.read_sql(self.sql.text, conn)
        # convert any date columns to datetime (altair only works with datetime NOT date)

        if len(df)==0:
            raise NoDataException('Query returned no data.  Please check your query and try again')
        for col in df.columns:
            try:
                if type(df[col][0]) == datetime.date:
                    df[col] = pd.to_datetime(df[col])
            except:
                raise Exception(f'Column error - do you have multiple columns with the same name?')                    
        if self.cache:
            params = vars(self)
            if not os.path.isdir(os.path.dirname(self.get_cache_files()[0])):
                os.mkdir(os.path.dirname(self.get_cache_files()[0]))
            df.to_csv(self.get_cache_files()[0], index=False)
            with open(self.get_cache_files()[1],'w+') as fobj:
                fobj.write(self.sql.text)
            self.csv=self.get_cache_files()[0]
        self.df=df
        return self.df
    
    def test(self, json_file=''):
        """
        applies tests defined in json/data_tests and returns test results which include a summary of failed records
        and copies of those records.  Output is saved in self.tests
        """
        assert len(self.df)>0,"Query object has no dataframe to test - try Query.run() or Query.load() to produce one"
        assert json_file!='', "you need to specify a json file (which lives in workspace/json/data_tests)"
        df=self.df
        workspace_dir, workspace_name = get_ws()
        if ".json" not in json_file.lower():
            json_file = json_file + ".json"
        full_json_file = os.path.join(workspace_dir, workspace_name, 'tests/json',json_file)
        with open(full_json_file,'r') as fobj:
            x=json.load(fobj)
            tests=x['tests']  
            def replace_with_df(match):
                return f"df[\'{match.group(1)}\']"  
            test_report = {}
            for test in tests:
                print(f'Checking {test["name"]}')
                pattern = r"'(.*?)'"
                modified_text = re.sub(pattern, replace_with_df, test['assert'])                
                # print(modified_text)
                if ("==" in modified_text) and ("`" not in modified_text):                    
                    # need to deal with pandas NaN!=NaN - ridiculous!
                    splits = modified_text.split("==")
                    assert len(splits)==2, "you cannot have more than one '==' in your assertion.  Check your json test file."
                    lhs = eval(splits[0].strip())
                    rhs = eval(splits[1].strip())
                    if type(lhs)==pd.core.series.Series:
                        lhs=lhs.fillna(-99999)
                    if type(rhs)==pd.core.series.Series:
                        rhs=rhs.fillna(-99999)    
                    tfs = lhs == rhs
                else:    
                    modified_text = modified_text.replace("`","'")
                    tfs = eval(modified_text)
                if type(tfs)==pd.core.series.Series:
                    fails = sum(tfs==False)
                    print('Number of records which failed: ',fails)
                    print('Percentage of records that failed: ',str(100*fails/len(df))+'%')
                    if fails>0:
                        print('Failed records:')
                        print(df[tfs==False])
                        test_report[test["name"]] = {
                            "fails": fails,
                            "percentage_fails": 100*fails/len(df),
                            "failed_records": df[tfs==False]
                        }
                    else:
                        test_report[test["name"]] = "All Passed!"   
                elif type(tfs)==np.bool_:
                    if tfs:
                        print('Test passed!')
                        test_report[test["name"]] = "Passed"
                    else:
                        print('Test failed!')
                        test_report[test["name"]] = "Failed"    
            self.tests[json_file.replace('.json','')] = test_report        



    def write_sql(self, table, warehouse=get_warehouse(), database=get_database(), schema=get_schema(), append=False, write_timestamp=True, unique='',**kwargs):
        """
        writes result to sql table.  Note: only works for Snowflake at the moment.  If the table does not exist
        then one is automatically created, which may result in fields being of an unexpected type (eg dates are 
        often converted to integers by Snowflake).  To avoid unexpected results, it is advised to create your table 
        in advance.

        This works by opening a new db connection where you can specify the optional params:

        table - table name    
        warehouse - warehouse
        database - database
        schema - schema
        append - append to an existing table (default False)                
        write_timestamp - if True then any date / datetime columns are written to SQL as timestamps, if False then written as dates
        """
        assert len(self.df)>0,"Query object has no dataframe - try Query.run() or Query.load() to produce one"
        if append==False:
            auto_create_table=False
        assert table, "you must specify a table name - doesn't matter if it exists already or not"

        # SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
        # if not warehouse:
        #     warehouse=f"{SNOWFLAKE_ROLE}_QUERY_LARGE_WH"
        # if not database:
        #     database = "LYST"
        # if not schema:
        #     if "WRITE_SCHEMA" not in os.environ:
        #         schema = f"{database}_ANALYTICS"
        #     else:
        #         if os.environ["WRITE_SCHEMA"]=='':
        #             schema = f"{database}_ANALYTICS"
        #         else:
        #             schema = os.environ["WRITE_SCHEMA"]
        schema=schema.upper() 
        warehouse=warehouse.upper() 
        database=database.upper()                    
        conn = py_connect_db(warehouse=warehouse, database=database, schema=schema)
        
        df=self.df
        for idx,dtype in enumerate(df.dtypes):
            if 'date' in str(dtype).lower():
                col = df.columns[idx]
                if write_timestamp==True:
                    df[col] = df[col].dt.strftime('%Y-%m-%d:%H-%M-%S')
                else:
                    df[col] = df[col].dt.strftime('%Y-%m-%d')

        def get_table_metadata(df,**kwargs):            
            def map_dtypes(x):
                if (x == 'object') or (x=='category'):
                    return 'VARCHAR'
                elif 'bool' in x:
                    return 'BOOLEAN'
                elif 'date' in x:
                    return 'DATE'
                elif 'int' in x:
                    return 'NUMERIC'  
                elif 'float' in x: return 'FLOAT' 
                else:
                    print("cannot parse pandas dtype")
            sf_dtypes = [map_dtypes(str(s)) for s in df.dtypes]
            cols=[c.upper() for c in df.columns]
            if kwargs:
                for key in kwargs:
                    if key.upper() in cols:
                        sf_dtypes[cols.index(key.upper())] = kwargs[key].upper()

            table_metadata = ", ". join([" ".join([y.upper(), x]) for x, y in zip(sf_dtypes, list(df.columns))])

            return table_metadata


        def df_to_snowflake_table(table_name, operation, df, conn=conn, unique=unique, **kwargs): 
            if operation=='create_replace':
                df.columns = [c.upper() for c in df.columns]
                table_metadata = get_table_metadata(df,**kwargs)
                conn.cursor().execute(f"CREATE OR REPLACE TABLE {table_name} ({table_metadata})")

                if unique:
                    assert unique in df.columns, f'"{unique}" is not in the dataframe columns'
                    df = df.drop_duplicates(subset=[unique])    
                write_pandas(conn, df, table_name.upper())
            elif operation=='insert':
                if unique:
                    assert unique in df.columns, f'"{unique}" is not in the dataframe columns'
                    df = df.drop_duplicates(subset=[unique])

                table_rows = str(list(df.itertuples(index=False, name=None))).replace('[','').replace(']','')

                if unique:
                    cols = df.columns
                    temp_table = f't('
                    for c in cols:
                        temp_table+=c+','
                    temp_table = temp_table[:-1] + ')'                        
                    sql_statement = f"""
                        INSERT INTO {table_name}
                        SELECT *
                        FROM (VALUES
                            {table_rows}
                        ) AS {temp_table}
                        -- Make sure the table doesn't already contain the IDs we're trying to insert
                        WHERE {unique} NOT IN (
                        SELECT {unique} FROM {table_name}
                        )
                        -- Make sure the data we're inserting doesn't contain duplicate IDs
                        -- If it does, only the first record will be inserted (based on the ORDER BY)
                        -- Ideally, we would want to order by a timestamp to select the latest record
                        QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY {unique}
                        ORDER BY {unique} ASC
                        ) = 1;
                    """
                else:
                    sql_statement = f"INSERT INTO {table_name} VALUES {table_rows}"
                conn.cursor().execute(sql_statement)

        operation = 'create_replace'
        if append==True:
            operation = 'insert'

        df_to_snowflake_table(table, operation, df, conn=conn, unique=unique, **kwargs)

        # success, nchunks, nrows, output  = write_pandas(conn=conn,df=self.df,table_name=table,database=database,schema=schema,overwrite=True,quote_identifiers=False,auto_create_table=True)

class Test(Query):
    """
    DQT Test Class

    Use this class to test data using SQL templates, ala DBT

    Test templates live in workspace/tests/sql
    """
    def __init__(self, template=''):
        super().__init__(query=template)
        assert template[-4:]=='.sql', "you need to input a '.sql' template file"
        assert self.template[:5]=='test_', "your test template needs to start with 'test_'"
        super().__init__(query=template)

    def run(self):
        self.test_result='FAILED'
        try:
            super().run()
            self.test_details=self.df
        except NoDataException:
            self.test_result='PASSED'        
        
        return self.test_result
    
def get_global_template_dir():
    return os.path.join(str(Path(__file__).parents[0]),'sql/templates/')
def get_global_macros_dir():
    return os.path.join(str(Path(__file__).parents[0]),'sql/templates/macros/')
# def get_global_includes_dir():
#     return os.path.join(str(Path(__file__).parents[0]),'sql/templates/includes/')


def get_user_template_dir():
    ws_root, ws_name = get_ws()
    return os.path.join(ws_root,ws_name,'templates')
    # return os.path.join(str(Path(__file__).parents[1]),'user/templates/')

def get_user_tests_template_dir():
    ws_root, ws_name = get_ws()
    return os.path.join(ws_root,ws_name,'tests','sql')

def get_user_macros_dir():
    ws_root, ws_name = get_ws()
    return os.path.join(ws_root,ws_name,'templates/macros')
    # return os.path.join(USER_DIR,'templates/macros/')

def get_user_includes_dir():
    ws_root, ws_name = get_ws()
    return os.path.join(ws_root,ws_name,'templates/includes')
    # return os.path.join(USER_DIR,'templates/includes/')

def compile(template='total_aggs.sql',*args,**kwargs):
    """
    creates sql queries by injecting into template
    or takes a string query and substitues params
    """

    environment = Environment(loader=FileSystemLoader([
        get_user_template_dir(),
        get_user_tests_template_dir(),
        get_global_template_dir(),
        get_user_macros_dir(),
        get_global_macros_dir(),
        get_user_includes_dir()
        # get_global_includes_dir()
    ]))
    custom_filters = [f for _, f in filters.__dict__.items() if callable(f)]
    custom_filters = [f for f in custom_filters if f.__name__[:4]=='dqt_']
    for f in custom_filters:
        # environment.filters['dqt_combinations'] = filters.dqt_combinations
       environment.filters[f.__name__] = f

    if 'select' in template.lower():
        s=template
        if len(args)>0:
            for i,arg in enumerate(args):
                s=s.replace(f'${1+i}',arg)
        
        pattern = r'macros\.[\w]+\('
        r=re.search(pattern,s)

        if ('{%' in s) or (r!=None):
            filename = str(uuid.uuid4()) + '.sql'
            full_filename = os.path.join(get_user_template_dir(),filename) 
            if (r!=None):
                s = "{% import 'macros.jinja' as macros %}\n" + s
                s = "{% import 'mymacros.jinja' as mymacros %}\n" + s

            with open(full_filename, 'w+') as f:
                f.write(s)
            template = environment.get_template(filename)
            os.remove(full_filename)

            rendered_str = template.render(kwargs)
            pattern = '\'[A-Za-z0-9_.-\/]+\.csv\''
            m=re.findall(pattern, rendered_str)
            if len(m)>0:
                rendered_str=rendered_str.replace(m[0],f'read_csv_auto({m[0]}, header=true)')    
            return (False,rendered_str)
        else:
            for key,val in kwargs.items():
                s=s.replace('{{' + key + '}}',val)
            rendered_str=s                
            pattern = '\'[A-Za-z0-9_.-\/]+\.csv\''
            m=re.findall(pattern, rendered_str)
            if len(m)>0:
                rendered_str=rendered_str.replace(m[0],f'read_csv_auto({m[0]}, header=true)')    
            return (False,rendered_str)            
    else:
        template = environment.get_template(template)
        rendered_str = template.render(kwargs)
        pattern = '\'[A-Za-z0-9_.-\/]+\.csv\''
        m=re.findall(pattern, rendered_str)
        if len(m)>0:
            rendered_str=rendered_str.replace(m[0],f'read_csv_auto({m[0]}, header=true)')    
        return (True,rendered_str)