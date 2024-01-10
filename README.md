# PYDQT - Python Data Query Tool
A project which aides in querying of both local and cloud based relational, tabular data.
Some things it can do:

- run paramterised SQL commands; both strings and templates
- create and reuse jinja based templates to simplify long, complex queries
- easily create and store jinja macros
- run queries remotely on snowflake or locally via duckdb
- cache results locally for further processing and analysis
- seamlessly return results as a pandas dataframe
- test data on single columns or combinations of columns

## Installation
You can install from PyPI:

```
pip install pydqt
```

or 

```
poetry add pydqt
```

Or you can fork the [github repo](https://github.com/markingham77/dqt) 

PYDQT works with local data straight out of the box.  PYDQT also has support to work with remote Snowflake servers.  To enable this, one final step is required - you need to provide your credentials so PYDQT can connect to the remote servers.  

When you first import pydqt, it will create a .env file in the project root and you have to fill in the blanks.

To do this use the **env_edit** utility:

```
from pydqt import env_edit

env_edit()
```

A text editor will then open, allowing you to fill in the blanks:

```bash
SNOWFLAKE_LOGIN = ''
SNOWFLAKE_ROLE = ''
SNOWFLAKE_DEFAULT_DATABASE = ''
SNOWFLAKE_DEFAULT_SCHEMA = ''
WORKSPACE_ROOT = ''
WORKSPACE_NAME = ''
```

When done, save and close the file then reload to ensure your changes are loaded into the appropriate environment variables:

```
from pydqt import env_reload

env_reload()
```

The SNOWFLAKE credentials are only necessary if you want to query snowflake and the .env should not be committed to a repo.  Without the .env variables, PYDQT will still work fine with local data.

## Testing
PYDQT comes with some tests, which can be run from within vscode or the command line.  They use pytest so if running from vscode, ensure that you configure the pytest framework.  From the command line (ensure you're in the root of the project) type:

```
python -m pytest
```

**If you installed PYDQT from pypi then you will have to change directory to the DQT package directory (probably within site-packages) and run the tests from there.**

You should, hopefully see lots of green (ie tests passing).  If you do not have the snowflake or looker variables defined (see above) then you will see tests related to those failing but all other tests should pass.

## PYDQT Main Class; Query
PYDQT has one main class - Query.

Query requires at least one input - a query, can be sql filename or a direct select query.  Upon instantiation, Query creates an object with various fields and methods relating to this query.  Note that Query does not automatically run a query but it does automatically compile the query.  To run the query you use the .run() method (see examples below).  In addition to querying data, Query can also write data to SQL.  Using the write_sql() method, users can write the .df property to a table of their choosing (and one is created if it doesn't already exist).  See example below. 

Once a query has been run, you can also test the data using pre-specifed tests declared in a json file in the json/data_tests subfolder of your current workspace.  See example below.

All the examples assume you are using PYDQT from within an interactive python session, such as jupyter or hex.  There is a notebooks folder within the project where you can also find examples.  You can create your own notebooks here and they will be ignored by git (so long as they don't end in "_example.ipynb")

## PYDQT Workspaces
PYDQT encourages the use of using workspaces, which are self-contained directories containing everything that the Query class needs to compile a valid SQL statement.  Workspaces consist of two directories:

- templates: this contains SQL templates, includes and macros
- cache: this contains cached data and their associated compiled SQL statements

### Determining and setting your workspace
Your workspace is determined by the values of the **WORKSPACE_ROOT**  and **WORKSPACE_NAME** environment variables.  **WORKSPACE_NAME** is a sub-directory of **WORKSPACE_ROOT**.  The default values are a folder named "workspaces" in the folder where ydqt is installed (probably site-packages) and "main" for ROOT and NAME respectively.

This default is probably not what you want.  In many cases it is preferable to have your workspaces seperate from the site-packages where pydqt was installed, often it is more convenient to have your workspaces somewhere under $HOME.  You can use the env_edit and env_reload utilities to change your workspace but PYDQT provides two shortcut functions to facilitate this; **set_workdir** and **workspace**:

```
from pydqt import set_workspace

set_workspace(root='/tmp', name='research')
```

If the workspace does not already exist, PYDQT will create the necessary folders for a valid workspace.  The workspace function then points PYDQT to the correct workspace.  The above commands will result in the following directory in /tmp:

```
── research
    ├── cache
    │   └── snowflake
    ├── json
    │   ├── data_tests
    │   └── tables
    └── templates
        ├── compiled
        ├── includes
        └── macros
          └── mymacros.jinja
```

As you can see there are no templates in this workspace as it's just been created.  Your own custom templates go in the templates folder.  For example, here's the structure for the default "main" workspace that ships with PYDQT:

```
.
└── main
    ├── cache
    │   └── snowflake
    ├── json
    │   ├── data_tests
    │   └── tables
    └── templates
        ├── compiled
        ├── includes
        └── macros
            └── mymacros.jinja
```

This is how your workspace should look (though you will probably have many more template .sql files in there).


## Query Examples
### Example 1: simple parameterized sql query string
```
# import the Query class
from pydqt import Query, test_data_file_full_path

query = Query(query="select * from {{table}} limit 10;",table=test_data_file_full_path())
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
There are example templates in [workspaces/main/templates](workspaces/main/templates).  You can create your own templates in your desired workspace [workspaces/main/templates](workspaces/main/templates).  Feel free to copy the examples into here and hack away.  PYDQT searches for templates and any includes in your workspace.  Let's use the [test.sql](workspaces/main/templates/test.sql) template.  Here's a preview:
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

Because PYDQT uses jinja (the same templating technology behind DBT) then the sky is the limit in terms of complexity - for example, you can use for loops, conditionals and macros to concoct sql queries.  However, you will always be able to see the compiled SQL, by looking at the .sql property of the Query object:

```
q.sql
```

There is also a convenience method, .open(), to open the query in an editor (useful for long queries):
```
q.sql.open()
```


### Example 3: Macros
[Macros](https://jinja.palletsprojects.com/en/3.1.x/templates/#macros) are jinja constructs that allow you to write custom functions inside SQL.  [PYDQT macros](sql/templates/macros/macros.jinja) has some global macros: sql/templates/macros/macros.jinja and you can create your own local macros in mymacros.jinja (in the macros folder of your workspace, see above).  To use macros in your template, you first have to import them at the top of your jinja template, like so:

```
{% import 'macros.jinja' as macros %}
{% import 'mymacros.jinja' as mymacros %}
```

[base.sql](workspaces/main/templates/base.sql) imports these macros so if you extend a template from base.sql then you automatically import them.  If importing by hand, PYDQT finds the macro files automatically so you do not need full path names.  To actually use them you reference them like so:

<pre>
select 
    *,
    {{ <b>macros.ma</b>('gmv',4,order='dates',partition='region,source') }} as ma_4
from {{table}}
</pre>

See the [test.sql](workspaces/main/templates/test.sql) template for an example of using macros within a template.

### Example 4: writing results to a SQL table
Get some data:

<pre>
query = Query(query="select * from '{{table}}' limit 10;",table=test_data_file_full_path())
query.run()
</pre>

Now we call the write_sql() method:

<pre>
query.write_sql("my_table", schema="SCHEMA_NAME", append=False, timestamp=False)
</pre>

See write_sql help for more details

### Example 5: testing data
DQT can test data for simple tests such as non null as well as testing combinations of columns

Get some data:

<pre>
query = Query(query="select * from '{{table}}';",table=test_data_file_full_path())
query.run()
</pre>

the test data looks like this:

<pre>
      dates	  orders	gmv	  region	source
0	2022-01-31	94	584.37	US	css
1	2022-02-28	63	5212.05	US	css
2	2022-03-31	70	2304.68	US	css
3	2022-04-30	60	2604.20	US	css
4	2022-05-31	13	737.94	US	css
...	...	...	...	...	...
</pre>

Now let's add a coulpe of columns to gives you a flavour on how to combine columns in tests:

<pre>
query.df['gmv_per_order'] = query.df['gmv']/query.df['orders']
query.df['wgts'] = query.df['gmv']/query.df['gmv'].sum()
</pre>

We can use query.test() to test data.  First we need a json file which contains our tests.  Tests go in 
the json/data_tests sub-folder of your current workspace:

<pre>
.
└── main
    ├── cache
    │   └── snowflake
    ├── json
    │   ├── data_tests <-- JSON TEST FILES GO IN HERE
    │   └── tables
    └── templates
        ├── compiled
        ├── includes
        └── macros
            └── mymacros.jinja
</pre>

For our example, we are going to use the json below, which should result in three passes (1,2 nd 5) and two fails (3 and 4).  Also, note in the last test how `css` is quoted using the backtick quote.  All strings should be quoted this way, as the single-quote is reserved for dataframe column names.  

Behind the scenes, dqt fields are materialised as pandas Series objects and so a test can be comprised of any Series methods which return either a Series or a scalar as well as the usual pandas scalar operators.  For example, the first three tests below will return a Series of booleans.  The last two will return a single boolean.  The last test is perhaps the most complex as it combines two Series aggregation methods with a boolean logical operator (and neatly shows how you may have to be careful with rounding when using pandas dataframe methods).

<pre>
{
    "tests":[
        {
            "name": "gmv_per_order_check",
            "assert": "'gmv_per_order'=='gmv'/'orders'"
        },
        {
            "name": "orders_are_non_negative",
            "assert": "'orders'>=0"
        },
        {
          "name": "orders_come_from_css",
          "assert": "'source'==`css`"
        },
        {
         "name": "wgts_sum_to_one",
         "assert": "'wgts'.sum()==1"
        },
        {
         "name": "wgts_sum_to_one_within_epsilon",
         "assert": "('wgts'.sum()>0.9999) & ('wgts'.sum()<=1.0001)"
        }
    ]
}
</pre>
save this as in the above sub-folder as 'example.json' and run:

<pre>
query.test('example.json')
</pre>

The above tests will run and the resulting test report can be found in query.tests:

<pre>
query.tests

{'example': {'gmv_per_order_check': 'All Passed!',
  'orders_are_non_negative': 'All Passed!',
  'orders_come_from_css': {'fails': 378,
   'percentage_fails': 75.0,
   'failed_records':          dates  orders      gmv region  source  gmv_per_order      wgts
   21  2022-01-31      98  7306.74     US  direct      74.558571  0.005900
   22  2022-02-28      82  8150.29     US  direct      99.393780  0.006582
   23  2022-03-31      37  3478.29     US  direct      94.007838  0.002809
   24  2022-04-30      50  4803.71     US  direct      96.074200  0.003879
   25  2022-05-31      23  1773.99     US  direct      77.130000  0.001433
   ..         ...     ...      ...    ...     ...            ...       ...
   499 2023-05-31      91  2774.43     FR     app      30.488242  0.002240
   500 2023-06-30      82  8066.01     FR     app      98.365976  0.006514
   501 2023-07-31      80  6615.54     FR     app      82.694250  0.005342
   502 2023-08-31      82  1030.11     FR     app      12.562317  0.000832
   503 2023-09-30      57  3772.19     FR     app      66.178772  0.003046
   
   [378 rows x 7 columns]},
  'wgts_sum_to_one': 'Failed',
  'wgts_sum_to_one_within_epsilon': 'Passed'}}
</pre>

As we can see the thrid test had many records which failed as they're not from css.  You can also see that tests 
which return a boolean do not have record level detail attached in the test result.



## Quality of cached data

Finally, PYDQT has an inbuilt check before loading cached data, that the current compiled SQL matches the SQL that actually produced the cached data.  The only data that are cached are produced by templates that reference remote data.  Any results produced by queries involving local data will not be cached (users can always save the .df property of the Query object using .to_csv() or some other pandas dataframe "to_" method).


## Acknowledgements and contributions
PYDQT was inspired in part by dbt and it's paramterized approach to SQL.  PYDQT aims to help bring more rigour to analysis by making calls to the database more DRY and organised, with a clear audit trail of how data was selected.

If anyone wants to contribute then please feel free to fork and PR!
