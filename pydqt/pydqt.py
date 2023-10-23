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
from pprint import pprint
import uuid
import duckdb
from duckdb import CatalogException
from pathlib import Path
import shutil

def test_data_file_full_path():
    return os.path.join(Path(__file__).parents[1],'test.csv')

def test_data_exists():
    test_data_file = os.path.join(Path(__file__).parents[1],'test.csv')
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
    outfile = os.path.join(Path(__file__).parents[1],'test.csv')
    df = df.round({'orders': 0, 'gmv': 2})    
    df['orders'] = df['orders'].astype(int) 
    df.to_csv(outfile, index=False)    
    
                      
    return df



def set_workdir(name):
    """
    sets the working directory.  You probably don't want the default, which is a subdir
    of dqt within site-packages.  You can set this to be something more memorable, such
    as '/User/<user>/Documents'
    """
    outfile = open("new.env", "w") 
    has_work_dir=False
    for line in open(os.path.join(Path(__file__).parents[1],'.env'), "r"): 
        if line.strip().startswith('WORK_DIR'):
            newline = f'WORK_DIR = \'{name}\''
            outfile.write(newline) # write in new file
            has_work_dir=True
        else:    
            outfile.write(line) # write in new file
    if has_work_dir==False:
        outfile.write(f'\nWORK_DIR = \'{name}\'')
    outfile.close() 
    shutil.copyfile(os.path.join(Path(__file__).parents[1],'.env'), 'bak.env')
    os.remove(os.path.join(Path(__file__).parents[1],'.env'))
    shutil.copyfile('new.env', os.path.join(Path(__file__).parents[1],'.env'))
    os.remove('new.env')
    os.remove('bak.env')

    load_dotenv(
            Path(find_dotenv()),
            override=True
        ) 
    setup_local_dirs()

def workspace(name):
    """
    sets or creates the workspace for the provided name.  If not an existing workspace 
    then one is created.  This function sets the environment variable, "WORKSPACE" and 
    changes its value in the .env file
    """
    outfile = open("new.env", "w") 
    for line in open(os.path.join(Path(__file__).parents[1],'.env'), "r"): 
        if line.strip().startswith('WORKSPACE'):
            newline = f'WORKSPACE = \'{name}\''
            outfile.write(newline) # write in new file
        else:    
            outfile.write(line) # write in new file
    outfile.close() 
    shutil.copyfile(os.path.join(Path(__file__).parents[1],'.env'), 'bak.env')
    os.remove(os.path.join(Path(__file__).parents[1],'.env'))
    shutil.copyfile('new.env', os.path.join(Path(__file__).parents[1],'.env'))

    load_dotenv(
        Path(find_dotenv()),
        override=True
    ) 
    setup_local_dirs()


def env_reload():
    load_dotenv(
        Path(find_dotenv()),
        override=True
    ) 
    setup_local_dirs()

def env_edit():
    filename = os.path.join(Path(__file__).parents[1],'.env')
    os.system(f'open {filename}') 

    load_dotenv(
        Path(find_dotenv()),
        override=True
    ) 
    setup_local_dirs()

    # load_dotenv(
    #         Path(find_dotenv())
    #     )  # find .env automagically by walking up directories until it's found
    

def setup_local_dirs():
    load_dotenv(
            Path(find_dotenv())
        )  # find .env automagically by walking up directories until it's found

    if "WORK_DIR" not in os.environ:
        ws_dir = os.path.join(Path(__file__).parents[1],'workspaces')
    else:
        ws_dir = os.environ['WORK_DIR']    
    if not os.path.exists(ws_dir):
        os.mkdir(ws_dir)
    if "WORKSPACE" not in os.environ:
        user_dir = os.path.join(ws_dir,'main')
        if not os.path.exists(user_dir):
            os.mkdir(user_dir)
    else:        
        user_dir = os.getenv("WORKSPACE")    
        if len(user_dir)==0:
            user_dir = os.path.join(ws_dir,'main')
        else:
            if "/" not in user_dir:
                user_dir = os.path.join(ws_dir,user_dir)    
    if not os.path.exists(user_dir):
        os.mkdir(user_dir)

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

    # # set up user templates and ca  
    # if not os.path.exists(os.path.join(Path(__file__).parents[1],'user')):
    #     os.mkdir(os.path.join(Path(__file__).parents[1],'user'))
    # if not os.path.exists(os.path.join(Path(__file__).parents[1],'user/templates')):
    #     os.mkdir(os.path.join(Path(__file__).parents[1],'user/templates'))
    # if not os.path.exists(os.path.join(Path(__file__).parents[1],'user/templates/compiled')):
    #     os.mkdir(os.path.join(Path(__file__).parents[1],'user/templates/compiled'))
    # if not os.path.exists(os.path.join(Path(__file__).parents[1],'user/templates/includes')):
    #     os.mkdir(os.path.join(Path(__file__).parents[1],'user/templates/includes'))
    # if not os.path.exists(os.path.join(Path(__file__).parents[1],'user/templates/macros')):
    #     os.mkdir(os.path.join(Path(__file__).parents[1],'user/templates/macros'))
    # if not os.path.exists(os.path.join(Path(__file__).parents[1],'user/cache')):
    #     os.mkdir(os.path.join(Path(__file__).parents[1],'user/cache'))

    # if not os.path.exists(os.path.join(Path(__file__).parents[1],'user/cache/snowflake')):
    #     os.mkdir(os.path.join(Path(__file__).parents[1],'user/cache/snowflake'))

def setup_env():
    if not os.path.exists(os.path.join(Path(__file__).parents[1],'.env')):
        print('.env does not exist - creating unfilled .env file')
        with open(os.path.join(Path(__file__).parents[1],'.env'),'w') as f:
            f.write("""SNOWFLAKE_LOGIN = ''
SNOWFLAKE_ROLE = ''
WORK_DIR = ''
WORKSPACE = ''""")

user_dir = setup_local_dirs()
setup_env()
if not test_data_exists():
    create_test_data()

def py_connect_db() -> snowflake.connector.connection.SnowflakeConnection:
    """connect to snowflake, ensure SNOWFLAKE_LOGIN defined in .env"""

    load_dotenv(
        Path(find_dotenv())
    )  # find .env automagically by walking up directories until it's found

    if "SNOWFLAKE_LOGIN" not in os.environ:
        raise EnvironmentError(
            "Failed. Please set SNOWFLAKE_LOGIN=<lyst email> & SNOWFLAKE_ROLE in your .env file"
        )
    SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

    return snowflake.connector.connect(
        account="fs67922.eu-west-1",
        user=os.getenv("SNOWFLAKE_LOGIN"),
        authenticator="externalbrowser",
        database="LYST",
        schema="LYST_ANALYTCS",
        role=SNOWFLAKE_ROLE,
        warehouse=f"{SNOWFLAKE_ROLE}_QUERY_LARGE_WH",
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
            else:
                fn = fn + '__' + key + '__' + val

    return os.path.join(user_dir,'cache/snowflake/',fn)
    # return os.path.join(str(Path(__file__).parents[1]),'user/cache/snowflake/',fn)

def temp_sql_compiled_template_dir():
    return os.path.join(user_dir,'templates/compiled')
    # return os.path.join(str(Path(__file__).parents[1]),'user/templates/compiled')

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

    def run(self):
        if '.csv' in self.sql.text:
            df = duckdb.sql(self.sql.text).df()
        else:
            conn = py_connect_db()
            df = pd.read_sql(self.sql.text, conn)
        # convert any date columns to datetime (altair only works with datetime NOT date)

        if len(df)==0:
            raise Exception('Query returned no data.  Please check your query and try again')
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

def get_global_template_dir():
    return os.path.join(str(Path(__file__).parents[0]),'sql/templates/')
def get_global_macros_dir():
    return os.path.join(str(Path(__file__).parents[0]),'sql/templates/macros/')
# def get_global_includes_dir():
#     return os.path.join(str(Path(__file__).parents[0]),'sql/templates/includes/')


def get_user_template_dir():
    return os.path.join(user_dir,'templates/')
    # return os.path.join(str(Path(__file__).parents[1]),'user/templates/')

def get_user_macros_dir():
    return os.path.join(user_dir,'templates/macros/')
    # return os.path.join(str(Path(__file__).parents[1]),'user/templates/macros/')

def get_user_includes_dir():
    return os.path.join(user_dir,'templates/includes/')
    # return os.path.join(str(Path(__file__).parents[1]),'user/templates/includes')

def compile(template='total_aggs.sql',*args,**kwargs):
    """
    creates sql queries by injecting into template
    or takes a string query and substitues params
    """

    environment = Environment(loader=FileSystemLoader([
        get_user_template_dir(),
        get_global_template_dir(),
        get_user_macros_dir(),
        get_global_macros_dir(),
        get_user_includes_dir()
        # get_global_includes_dir()
    ]))
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
    

def get_example_template_names():
    """
    gets the names of all example templates - used for testing
    """
    return [f for f in os.listdir(os.path.join(user_dir,'templates')) if f.endswith('.sql') and f!='test.sql']
