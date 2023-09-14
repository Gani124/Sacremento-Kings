import json
import psycopg2
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, DDL

input_file1 = 'international_box_player_season.json'
input_file2 = 'nba_box_player_season.json'

# Read data from JSON file
with open('player.json', 'r') as f2:
    players_data = json.load(f2)
third_df = pd.DataFrame(players_data)
third_df.insert(0, 'ID', range(1, len(third_df) + 1))
# third_df.to_csv('players_with_id', index=False)
#===============================================Db connections========================================================
db_params = {
    'dbname': 'kings_intern_db',
    'user': 'sai',
    'password': '3ks1x',
    'host': 'kings-intern-db.c0bvlnkcljbc.us-west-2.rds.amazonaws.com',
    'port': '5432'
}
#===============================================data transformation====================================================
def transform_data(input_file, prefix):
    with open(input_file, 'r') as f:
        json_data = json.load(f)
        
    data_frame = pd.DataFrame(json_data)
    data_frame['minutesround'] = data_frame['minutes'].apply(round)
    data_frame['performance_per_minute'] = data_frame['points'] / data_frame['minutes']
    data_frame['field_goals_attempt'] = data_frame['two_points_attempted'] + data_frame['three_points_attempted']
    data_frame['field_goals_attempt_percentage'] = (data_frame['two_points_attempted'] + data_frame['three_points_attempted']) / data_frame['field_goals_attempt']
    data_frame['field_goals_made'] = data_frame['two_points_made'] + data_frame['three_points_made']
    data_frame['field_goals_made_percentage'] = (data_frame['two_points_made'] + data_frame['three_points_made']) / data_frame['field_goals_made']
    data_frame['free_throw_percentage'] = data_frame['free_throws_made'] / data_frame['free_throws_attempted']
    data_frame['total_rebounds'] = data_frame['offensive_rebounds'] + data_frame['defensive_rebounds']
    data_frame['Assist_to_turnover_ratio'] = data_frame['assists'] / data_frame['turnovers']
    data_frame['offensive_rating'] = (data_frame['points'] + data_frame['possessions']) * 100
    data_frame['blocked_shots_ratio'] = data_frame['blocked_shots'] / data_frame['blocked_shot_attempts']
    data_frame['personal_fouls_ratio'] = data_frame['personal_fouls'] / data_frame['personal_fouls_drawn']
    data_frame['possession_ratio_percentage'] = ((data_frame['possessions'] + data_frame['estimated_possessions']) / data_frame['team_possessions']) * 100
    data_frame = data_frame.replace([np.inf, -np.inf], 0)
    data_frame = data_frame.fillna(method='ffill').fillna(method='bfill')
    data_frame = data_frame.drop_duplicates()
    data_frame = data_frame.add_prefix(prefix)

    if prefix == 'international_':
        def assign_id(row):
            matching_rows = third_df[
                (third_df['first_name'] == row['international_first_name']) & 
                (third_df['last_name'] == row['international_last_name'])
            ]
            if not matching_rows.empty:
                return matching_rows.iloc[0]['ID']  # Get the first matching ID
            return None

        data_frame['player_id'] = data_frame.apply(assign_id, axis=1)
        columns_to_drop_int = [0,1,8,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,44,45,51]
        data_frame = data_frame.drop(data_frame.columns[columns_to_drop_int], axis=1)
        # data_frame.to_csv(prefix + 'players.csv', index=False)
        return data_frame
    else:
        data_frame['nba_last_name'] = data_frame['nba_last_name'].str.split(' ').str[0]
        data_frame['nba_first_name'] = data_frame['nba_first_name'].str.lower()
        data_frame['nba_last_name'] = data_frame['nba_last_name'].str.lower()
        def assign_id(row):
            matching_rows = third_df[
                (third_df['first_name'] == row['nba_first_name']) & 
                (third_df['last_name'] == row['nba_last_name'])
            ]
            if not matching_rows.empty:
                return matching_rows.iloc[0]['ID']  # Get the first matching ID
            return None
        data_frame['player_id'] = data_frame.apply(assign_id, axis=1)
        columns_to_drop= [0,1,8,11,12,13,14,15,16,17,18,19,20,21,22,23,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,42,47,48]
        data_frame = data_frame.drop(data_frame.columns[columns_to_drop], axis=1)
        # data_frame.to_csv(prefix + 'players.csv', index=False)
        return data_frame
    
first_df=transform_data(input_file1, 'international_')
second_df=transform_data(input_file2, 'nba_')
#==================================creating tables and engine in to postgres=============================================
players_columns_with_constraints =      [   ('ID','serial PRIMARY KEY'),
                                            ('first_name', 'varchar(25)'),
                                            ('last_name', 'varchar(25)'),
                                            ('Birth_date', 'date')
                                        ]

international_columns_with_constraints =[   ('ID', 'serial PRIMARY KEY'),
                                            ('international_season','int'),
                                            ('international_season_type','varchar(25)'),
                                            ('international_league','varchar(25)'),
                                            ('international_team','varchar(25)'),
                                            ('international_games','int'),
                                            ('international_starts','int'),
                                            ('international_points','int'),
                                            ('international_usage_percentage','numeric'),
                                            ('international_true_shooting_percentage','numeric'),
                                            ('international_three_point_attempt_rate','numeric'),
                                            ('international_free_throw_rate','numeric'),
                                            ('international_total_rebounding_percentage','numeric'),	
                                            ('international_assist_percentage','numeric'),
                                            ('international_steal_percentage','numeric'),
                                            ('international_block_percentage','numeric'),
                                            ('international_turnover_percentage','numeric'),
                                            ('international_minutesround','int'),
                                            ('international_performance_per_minute','numeric'),
                                            ('international_field_goals_attempt','int'),
                                            ('international_field_goals_attempt_percentage','int'),
                                            ('international_field_goals_made','int'),
                                            ('international_field_goals_made_percentage','int'),	
                                            ('international_free_throw_percentage', 'numeric'),
                                            ('international_total_rebounds','int'),
                                            ('international_Assist_to_turnover_ratio','numeric'),
                                            ('international_offensive_rating','numeric'),
                                            ('international_blocked_shots_ratio','numeric'),
                                            ('international_personal_fouls_ratio','numeric'),
                                            ('international_possession_ratio_percentage','numeric'),
                                            ('player_id','int REFERENCES splayers(ID)')
                                        ]

nba_columns_with_constraints =          [   ('ID', 'serial PRIMARY kEY'),
                                            ('nba_season','int'),
                                            ('nba_season_type','varchar(25)'),
                                            ('nba_league','varchar(25)'),
                                            ('nba_team','varchar(25)'),
                                            ('nba_games','int'),
                                            ('nba_starts','int'),
                                            ('nba_points','int'),
                                            ('nba_plus_minus','int'),
                                            ('nba_deflections','int'),
                                            ('nba_loose_balls_recovered','int'),
                                            ('nba_plays_used','int'),
                                            ('nba_usage_percentage','numeric'),
                                            ('nba_true_shooting_percentage','numeric'),
                                            ('nba_three_point_attempt_rate','numeric'),
                                            ('nba_free_throw_rate','numeric'),
                                            ('nba_total_rebounding_percentage','numeric'),	
                                            ('nba_assist_percentage','numeric'),
                                            ('nba_steal_percentage','numeric'),
                                            ('nba_block_percentage','numeric'),
                                            ('nba_turnover_percentage','numeric'),
                                            ('nba_internal_box_plus_minus','int'),
                                            ('nba_minutesround','int'),
                                            ('nba_performance_per_minute','numeric'),
                                            ('nba_field_goals_attempt','int'),
                                            ('nba_field_goals_attempt_percentage','int'),
                                            ('nba_field_goals_made','int'),
                                            ('nba_field_goals_made_percentage','int'),	
                                            ('nba_free_throw_percentage', 'numeric'),
                                            ('nba_total_rebounds','int'),
                                            ('nba_Assist_to_turnover_ratio','numeric'),
                                            ('nba_offensive_rating','numeric'),
                                            ('nba_blocked_shots_ratio','numeric'),
                                            ('nba_personal_fouls_ratio','numeric'),
                                            ('nba_possession_ratio_percentage','numeric'),
                                            ('player_id','int REFERENCES splayers(ID)')
                                        ]

create_queries = {
    'splayers': f'CREATE TABLE splayers ({", ".join([f"{col_name} {col_type}" for col_name, col_type in players_columns_with_constraints])})',
    'internationals_players': f'CREATE TABLE internationals_players ({", ".join([f"{col_name} {col_type}" for col_name, col_type in international_columns_with_constraints])})',
    'nba_players': f'CREATE TABLE nba_players ({", ".join([f"{col_name} {col_type}" for col_name, col_type in nba_columns_with_constraints])})',
}

engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}')

with engine.connect() as connection:
    for table_name, create_query in create_queries.items():
        ddl = DDL(create_query)
        connection.execute(ddl)
#====================================pushing data into sql============================================================
def push_dataframe_to_sql(df, table_name, engine):
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"Data has been pushed to table {table_name}.")
#======================================calling pushing dataframe function==============================================
push_dataframe_to_sql(third_df, 'splayers', engine)
push_dataframe_to_sql(first_df, 'internationals_players', engine)
push_dataframe_to_sql(second_df, 'nbas_players', engine)