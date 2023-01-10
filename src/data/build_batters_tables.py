
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
except Exception:
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
    data = pd.DataFrame(
        data_fromtable,
        columns=sf.get_table_col_names(connection, f'{schema}.{table}'),
    )
    print(f'successfully loaded {schema}.{table} with {data.shape[0]} rows')
except psycopg.Error as e:
    print(e)

# * For a given year, calculcate the xBA for a batter for each game and load to a table
# -------------------------------------------------------
# get all statcast data for July 4th, 2017
from pybaseball import statcast
month_statcast = statcast(start_dt='2017-07-04', end_dt='2017-08-04')

# Get ID, year, and Team
from pybaseball.lahman import *


# At bats
def get_atbats(df):
    atbats = df.groupby(['game_date', 'batter', 'at_bat_number']).agg({'batter':'nunique'})
    atbats.columns = ['at_bats']
    atbats = atbats.reset_index()
    atbats = atbats.drop(columns=['at_bat_number'], axis=1).groupby(['game_date', 'batter']).sum(numeric_only=True).reset_index()
    return atbats

# Estimated Batting Average
def get_batter_eba(df):
    return df.groupby(['game_date', 'batter']).agg({'estimated_ba_using_speedangle':'median'}).reset_index().fillna(0)

# Get Player ID, year, and team
def get_player_year_team(df):
    game_year = df.game_date.dt.year.unique()[0]
    batting = pybaseball.lahman.batting()
    batting = batting.loc[batting['yearID'] == game_year]
    return batting[['playerID', 'yearID', 'teamID']]

batter_info = get_atbats(month_statcast).merge(get_batter_eba(month_statcast), on=['game_date', 'batter'], how='inner')
#batter_info.merge(data, how = 'inner', left_on='batter', right_on='key_mlbam')

# Join Fangraphs ID to Team
data_short = data[['key_bbref', 'key_mlbam']]
batting_team_info = get_player_year_team(month_statcast).merge(data_short, how='inner', left_on='playerID', right_on='key_bbref').drop(['key_bbref', 'playerID', 'yearID'], axis = 1)

# Finally, get date, batter mlbam, and teamID
batting_info = batter_info.merge(batting_team_info, how='inner', left_on='batter', right_on='key_mlbam').drop('key_mlbam', axis = 1)
batting_info.batter.unique()[0]


# get all of this season's batting data so far
import pybaseball
pybaseball.teams()

# People traded in the middle of the season testing
data.loc[data['name_last'] == 'Cruz'] # person traded in the middle of the season
batting_stats = batting_stats(2017, qual = 5)
batting_stats.loc[batting_stats['IDfg'] == 21711] # Bating stats
batting_info.loc[batting_info['batter'] == 665833]
