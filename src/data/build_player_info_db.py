"""
TITLE: Insert Into Player Detail DB
Purpose: To have a database locally to pull from and be able to quickly scan for player info

"""

# Working with PostgreSQL
# Installing in Python: hhttps://www.psycopg.org/psycopg3/docs/basic/install.html
import psycopg
from psycopg import sql

import sys
import pathlib
from pybaseball import chadwick_register
import pandas as pd

# Set base path
try:
    base_path = pathlib.Path(__file__).absolute().parents[2]
except:
    base_path = pathlib.Path.cwd().absolute()

# Add utils folder if not already in system path
if str(base_path) not in sys.path:
    print('adding path')
    sys.path.append(str(base_path))

# Import SQL Functions
from utils import sql_functions as sf

# * Create the connection
# -------------------------------------------------------
# Had to create this admin account via terminal
# https://www.sqlshack.com/setting-up-a-postgresql-database-on-mac/
# Psycopg3 connectionv
try:
    connection = psycopg.connect("dbname=mlb_project user=admin password=password host=localhost", autocommit=True)
except psycopg.OperationalError as e:
    print(e)

# * Create Cursor Object
# -------------------------------------------------------
cur = connection.cursor()

# * Clean Data
# -------------------------------------------------------
# get the register data
data = chadwick_register()
# Get rid of -1 key mlbam since that's our key in the table
data = data.loc[data['key_mlbam'] != -1]
data = data.loc[data['key_fangraphs'] != -1]
data = data.dropna(axis = 0, subset='key_mlbam')


# * Create Player Info Table
# -------------------------------------------------------
schema, table = 'players', 'chadwick_register'
if sf.table_exists(connection, schema, table):
    print('table exits, dont create')
    col_names = sf.get_table_col_names(connection, schema+'.'+table) # Get column names
    print(f"Columns: {col_names}")
else:
    try:
        cur.execute(sql.SQL("""CREATE TABLE {}.{} (
                                "name_last" varchar,
                                "name_first" varchar,
                                "key_mlbam" INT PRIMARY KEY,
                                "key_retro" varchar,
                                "key_bbref" varchar,
                                "key_fangraphs" INT,
                                "mlb_played_first" FLOAT,
                                "mlb_played_last" FLOAT
                                );""").format(sql.Identifier(schema),
                                              sql.Identifier(table))
                    )
    except psycopg.Error as e:
        print(e)
    
    
# * Insert Into Table
# -------------------------------------------------------
try:
    sf.insert_into(connection, schema, table, data)
except psycopg.Error as e:
    print(e)
        

# * Load Table
# -------------------------------------------------------
schema, table = 'players', 'chadwick_register'
try:
    cur.execute(f"SELECT * FROM {schema}.{table}")
    data_fromtable = cur.fetchall()
    data = pd.DataFrame(data_fromtable, columns = sf.get_table_col_names(connection, schema+'.'+table))
    print(f'successfully loaded {schema}.{table} with {data.shape[0]} rows')
except psycopg.Error as e:
    print(e)
    
# * Load Table
# -------------------------------------------------------
