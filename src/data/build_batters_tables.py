
"""
TITLE: Insert Into Player Detail DB
Purpose: To have a database locally to pull from and be able to quickly scan for player info

"""

# Working with PostgreSQL
# Installing in Python: hhttps://www.psycopg.org/psycopg3/docs/basic/install.html
import psycopg

import sys
import pathlib
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
    
# * For a given year, calculcate the xBA for a batter for each game and load to a table
# -------------------------------------------------------
# get all statcast data for July 4th, 2017
from pybaseball import statcast
july4_statcast = statcast('2017-07-04')
july4_statcast
