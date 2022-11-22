'''
Title: Gather Game Info
Author: Sam LaFell
Date: 11/21/2022
Purpose: Gather Game information into one DF

Granularity: Date/HT/VT -- pitcher xba, batter xba for both teams
'''

# Import Libraries
import pandas as pd
pd.set_option('display.max_columns', 50)
import pybaseball as pb
from pybaseball import statcast, statcast_pitcher
from datetime import datetime, timedelta

'''
Data to use:
Statcast
'''


def gather_game_info(game_day, team):
    data = statcast(start_dt=game_day, end_dt=game_day, team=team)
    if data.shape[0] == 0:
        return None
    else:
        return data

def second_team(first_team, game_info_df):
    ########## TO DO for next time, find a better way to grab the teams. This is close to being a good spot ####################
    # Found home team is Miami, run that
    teams = [v for k,v in game_info_df[['home_team', 'away_team']].to_dict().items()]
    team_list = []
    for dict_object in teams:
        team = [v for k,v in dict_object.items()]
        team_list.append(team)
    team_flat = [item for sublist in team_list for item in sublist]
    team2 = [team_iter for team_iter in team_flat if team_iter != first_team]
    return team2[0]

def identify_home_away_pitchers(full_game_df):
    
    away_pitcher = full_game_df.loc[(full_game_df['inning'] == 1) & (full_game_df['inning_topbot'] == 'Bot')].pitcher.unique().tolist()[0]
    home_pitcher = full_game_df.loc[(full_game_df['inning'] == 1) & (full_game_df['inning_topbot'] == 'Top')].pitcher.unique().tolist()[0]

    return home_pitcher, away_pitcher

def pitcher_last4weeks(game_day, lookback_period, pitchers:list):
    # Define ranges
    first_day_range = (datetime.strptime(game_day, '%Y-%m-%d') - timedelta(days = lookback_period)).strftime('%Y-%m-%d')
    last_day_range = (datetime.strptime(game_day, '%Y-%m-%d') - timedelta(days = 1)).strftime('%Y-%m-%d')
    
    # Pull Pitching Information
    pitcher_df = pd.DataFrame()
    for pitcher in pitchers:
        temp_pitch_df = statcast_pitcher(start_dt=first_day_range, end_dt=last_day_range, player_id = pitcher)
        pitcher_df = pd.concat([pitcher_df, temp_pitch_df])
    
    return pitcher_df

def aggregate_pitcherpreformance(pitcher_df):
    return pitchers_df[['game_date', 'pitcher', 'estimated_ba_using_speedangle']].dropna().groupby(['pitcher']).agg({'estimated_ba_using_speedangle':'median'}).reset_index()


# * Step 1. Define the parameters of our search
# Who do we want to look at
team = 'ATL'
game_day = '2018-05-13'


# * Step 2. Grab the information for the game (id, day, teams)
# Full Statcast Info for the first team of interest
first_team_df = gather_game_info(game_day, team) # this gives all the pitchers that ATL threw, not Miami, that has to be different.
# Describe some basic info like game_id, date, home, and away
game_info = first_team_df[['game_pk', 'game_date', 'home_team', 'away_team']].drop_duplicates()

# * Step 3. Identify the 2nd team, and grab the statcast df for the team as well
# Who is the 2nd team that wasn't initially listed?
second_team = second_team(team, game_info)

# If Team1 was queried in the first function pull, skip it. Then move to team 2.
second_team_df = gather_game_info(game_day, second_team)

# * Step 4. Join the previous two DFs to combine all statcast info
# Append the two dfs
full_game_df = pd.concat([first_team_df, second_team_df])

# * Step 5. Identify Pitchers
home_pitcher, away_pitcher = identify_home_away_pitchers(full_game_df)

# * Step 6. Identify Get all at bats for the pitchers the last x days
pitchers_df = pitcher_last4weeks(game_day, 28, [home_pitcher, away_pitcher])

# * Step 7. Aggregate into one DF
four_week_pitcher_aggregate = aggregate_pitcherpreformance(pitchers_df)
four_week_pitcher_aggregate