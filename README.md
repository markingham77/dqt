# DQT - Data Query Tool
A project which aides in querying of both local and cloud based relational, tabular data.
Some things it can do:

- run paramterised SQL commands; both strings and templates
- create and reuse jinja based templates to simplify long, complex queries
- easily create and store jinja macros
- run queries remotely on snowflake or locally via duckdb
- cache results locally for further processing and analysis
- seamlessly return results as a pandas dataframe

## Installation
First get the repo onto your local machine:

- Fork this repository
- Clone your forked repository

Second, install [dependencies](pyproject.toml) via poetry (like pip but with better package version management):

- ensure pyenv and poetry are installed
- then cd to the project folder and type "poetry install" (this creates a virtual environemnt in the project root and installs all dependencies in that environment)
- open a new terminal tab (ensure you are in project root) and type "poetry shell" - this sets the virtual environment
- type "code ." if you want to use vscode with this package

DQT should now be set up to work with local data.  For remote data (optional), one final step is required - you need to provide your credentials so DQT can connect to the remote servers, such as Snowflake.  

When you first import dqt, it will create a .env file in the project root and you have to fill in the blanks:

```bash
SNOWFLAKE_LOGIN = ''
SNOWFLAKE_ROLE = ''
WORK_DIR = ''
WORKSPACE = ''
```

The SNOWFLAKE credentials are only necessary if you want to query snowflake and the .env should not be committed to a repo.  Without the .env variables, DQT will still work fine with local data.

## Testing
DQT comes with some tests, which can be run from within vscode or the command line.  They use pytest so if running from vscode, ensure that you configure the pytest framework.  From the command line (ensure you're in the root of the project) type:

```
python -m pytest
```

You should, hopefully see lots of green (ie tests passing).  If you do not have the snowflake or looker variables defined (see above) then you will see tests related to those failing but all other tests should pass.

## DQT Main Class; Query
DQT has one main class - Query.

Query requires at least one input - a query, can be sql filename or a direct select query.  Upon instantiation, Query creates an object with various fields and methods relating to this query.  Note that Query does not automatically run a query but it does automatically compile the query. 

All the examples assume you are using DQT from within an interactive python session, such as jupyter or hex.  There is a notebooks folder within the project where you can also find examples.  You can create your own notebooks here and they will be ignored by git (so long as they don't end in "_example.ipynb")

## DQT Workspaces
DQT encourages the use of using workspaces, which are self-contained directories containing everything that the Query class needs to compile a valid SQL statement.  Workspaces consist of two directories:

- templates: this contains SQL templates, includes and macros
- cache: this contains cached data and their associated compiled SQL statements

### Determining and setting your workspace
Your workspace is determined by the values of the **WORK_DIR**  and **WORKSPACE** environmet variables.  **WORKSPACE** is a sub-directory of **WORK_DIR**.  The default value of **WORK_DIR** is a folder named "main" inside the "workspaces" folder of dqt.

This default is probably not what you want.  In many cases it is preferable to have your workspaces seperate from the site-packages where dqt was installed, for it is probably more convenient to have your workspaces somewhere under $HOME.    

DQT can change your working directory and the workspace name.  This offers an efficient way to have multiple workspaces associated with different projects:

```
from dqt import set_workdir, workspace

set_workdir('/tmp') # sets WORK_DIR to '/tmp'
workspace('research') # sets workspace to '/tmp/research'
```

Alternatively, you can edit the .env file.  DQT has a utility to do this, **env_edit**:

```
from dqt import env_edit, env_reload

dqt.env_edit() # will open the .env file in a text editor
dqt.env_reload() # you need to call this in order for any changes in the .env file to be take effect
```


## Query Examples
### Example 1: simple parameterized sql query string
```
# import the Query class
from dqt import Query, test_data_file_full_path

query = Query(query="select * from '{{table}}' limit 10;",table=test_data_file_full_path())
query
```

```
Class: Query (Data query tool)

sql: sql from string (len: 101)       
template: None
cache: False
df: None
is_cached: False
params: 
  table: <test_data_location>
```

To see the compiled sql, look at the sql property of the Query object,q:

```
q.sql

SELECT *
FROM read_csv_auto(<test_data_location>, header=TRUE)
LIMIT 10;
```

To run the query use .run()  or .load()
```
q.run()  # always runs the query on snowflake
q.load() # will load from cache if it can, otherwise from snowflake 
```

More specifically:
- load() will return data from a locally cached .csv file, if present, if not then it will call run()
- run() will run the query on snowflake and then run() will cache the result in a local csv file.

Both run() and load() also populate the .csv property of the Query object:

```
q.csv

<location of your workspace>/cache/snowflake/<template_name__args>/data.csv'
```

and they also populate the .df field of the Query object, which is pandas dataframe of the query result


### Example 2: parameterized sql query template
There are example templates in [workspaces/main/templates](workspaces/main/templates).  You can create your own templates in your desired workspace [workspaces/main/templates](workspaces/main/templates).  Feel free to copy the examples into here and hack away.  DQT searches for templates and any includes in your workspace.  Let's use the [test.sql](workspaces/main/templates/test.sql) template.  Here's a preview:
```
{% extends "base.sql" %}
{% block main %}
{% set this = ({
        "min_query_date": "2022-09-30",
        "max_query_date": "2023-09-30",
    })
%}

with stage as (
select dates, orders, gmv, region, source,
{{ macros.ma('gmv',4,order='dates',partition='region,source') }} as gmv_ma4
from '{{table}}'
where dates>'{{min_query_date |default(this.min_query_date) }}'
...
...
```

A few things here, relating to jinja.  Firstly this template extends a base templates, [base.sql](workspaces/main/templates/base.sql) which means it inherits some values from there.

There are also some default parameters set by the **{% set ... %}** clause at the top of template, which are then used by the **default** filter.  This ensures that the sql will compile even if not all parameters are set by the Query object, Although not used by the test.sql, you can also include SQL snippets via  **{% include ... %}**.  For more on jinja templating see the [jinja docs](https://jinja.palletsprojects.com/en/3.1.x/templates/#base-template).


To run the template:
```
q=Query('test.sql', min_query_date='2023-01-01', table=test_data_file_full_path())
q.run()
```
will run the query on the test.cs data from '2023-01-01' to '2024-12-31'.

Because DQT uses jinja (the same templating technology behind DBT) then the sky is the limit in terms of complexity - for example, you can use for loops, conditionals and macros to concoct sql queries.  However, you will always be able to see the compiled SQL, by looking at the .sql property of the Query object:

```
q.sql
```

There is also a convenience method, .open(), to open the query in an editor (useful for long queries):
```
q.sql.open()
```


### Example 3: Macros
[Macros](https://jinja.palletsprojects.com/en/3.1.x/templates/#macros) are jinja constructs that allow you to write custom functions inside SQL.  [DQT macros](sql/templates/macros/macros.jinja) are in a single file: sql/templates/macros/macros.jinja and you can create your own in the mymacros.jinja file in the macros folder of your workspace.  To use macros in your template, you first have to import them at the top of your jinja template, like so:

```
{% import 'macros.jinja' as macros %}
{% import 'mymacros.jinja' as mymacros %}
```

[base.sql](workspaces/main/templates/base.sql) imports these macros so if you extend a template from base.sql then you automatically import them.  If importing by hand, DQT finds the macro files automatically so you do not need full path names.  To actually use them you reference them like so:

<pre>
select 
    *,
    {{ <b>macros.ma</b>('gmv',4,order='dates',partition='region,source') }} as ma_4
from {{table}}
</pre>

See the [test.sql](workspaces/main/templates/test.sql) template for an example of using macros within a template.

## Quality of cached data

Finally, DQT has an inbuilt check before loading cached data, that the current compiled SQL matches the SQL that actually produced the cached data.  The only data that are cached are produced by templates that reference remote data.  Any results produced by queries involving local data will not be cached (users can always save the .df property of the Query object using .to_csv() or some other pandas dataframe "to_" method).


## Acknowledgements and contributions
DQT was inspired in part by dbt and it's paramterized approach to SQL.  DQT aims to help bring more rigour to analysis by making calls to the database more DRY and organised, with a clear audit trail of how data was selected.

If anyone wants to contribute then please feel free to fork and PR!
