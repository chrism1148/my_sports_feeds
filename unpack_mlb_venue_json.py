import json 
import unidecode
import unicodedata
import numpy as np
import pandas as pd
import glob
import os
import player_throw_hand
import player_bat_hand
import msf_player_name
import pprint
pp = pprint.PrettyPrinter(indent=4)


#load json object
year = 2020
path = '/Users/chrismccallan/documents/statis/mlb/venues/seasonal_venues-mlb-%d-regular.json' % (year)

for file in glob.glob(path):
	with open(file) as json_venues:
		loaded_json = json.load(json_venues)
		
		#create player_gamelog DataFrame
		seasonal_venues = pd.json_normalize(loaded_json['venues'])
		seasonal_venues.to_csv( '%d-seasonal-venues.csv' % (year))
		#define column/Series data types
		# player_gamelog_float_cols = player_gamelog.select_dtypes(include=['float64']).columns
		# player_gamelog_int_cols = player_gamelog.select_dtypes(include=['int']).columns
		# player_gamelog_str_cols = player_gamelog.select_dtypes(include=['object']).columns
		# #fill  missing values
		# player_gamelog.loc[:, player_gamelog_float_cols] = player_gamelog.loc[:, player_gamelog_float_cols].fillna(0.0)
		# player_gamelog.loc[:, player_gamelog_int_cols] = player_gamelog.loc[:, player_gamelog_int_cols].fillna(0)
		# player_gamelog.loc[:, player_gamelog_str_cols] = player_gamelog.loc[:, player_gamelog_str_cols].fillna('None')




	# # player_game_stats.to_csv(('gamelogs/%d/cleaned_csv/' + '%d' + '-cleaned') % (year, player_gamelog.gameDate), sep=',', encoding='utf-8', columns=header)
	# player_game_stats.to_csv(('/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/cleaned_csv/' + '%d' + '-cleaned.csv') % (year, gameDate), index=0, sep=',', encoding='utf-8')