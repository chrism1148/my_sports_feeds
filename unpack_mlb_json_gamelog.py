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
from colorama import Fore, Back, Style 
import pprint
pp = pprint.PrettyPrinter(indent=4)

#define ANSI Escape Sequence color printing for terminal output
def prRed(skk): print("\033[91m {}\033[00m" .format(skk)) 
def prGreen(skk): print("\033[92m {}\033[00m" .format(skk)) 
def prYellow(skk): print("\033[93m {}\033[00m" .format(skk)) 
def prLightPurple(skk): print("\033[94m {}\033[00m" .format(skk)) 
def prPurple(skk): print("\033[95m {}\033[00m" .format(skk)) 
def prCyan(skk): print("\033[96m {}\033[00m" .format(skk)) 
def prLightGray(skk): print("\033[97m {}\033[00m" .format(skk)) 
def prBlack(skk): print("\033[98m {}\033[00m" .format(skk))

#load json object
year = 2017
date = 20170406 # used for single file processing only
path = '/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/raw_json/*json' % (year)  # bulk processing path
# path = '/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/raw_json/daily_player_gamelogs-mlb-%d-regular-%d.json' % (year, year, date)  # single file processing path


for file in glob.glob(path):
	with open(file) as json_gamelog:
		loaded_json = json.load(json_gamelog)
		
		#create player_gamelog DataFrame
		player_gamelog = pd.json_normalize(loaded_json['gamelogs'])
		#define column/Series data types
		player_gamelog_float_cols = player_gamelog.select_dtypes(include=['float64']).columns
		player_gamelog_int_cols = player_gamelog.select_dtypes(include=['int']).columns
		player_gamelog_str_cols = player_gamelog.select_dtypes(include=['object']).columns
		#fill  missing values
		player_gamelog.loc[:, player_gamelog_float_cols] = player_gamelog.loc[:, player_gamelog_float_cols].fillna(0.0)
		player_gamelog.loc[:, player_gamelog_int_cols] = player_gamelog.loc[:, player_gamelog_int_cols].fillna(0)
		player_gamelog.loc[:, player_gamelog_str_cols] = player_gamelog.loc[:, player_gamelog_str_cols].fillna('None')

	
#extract strings from filenames for use in creating gameDate series
	json_filename = str.split(file, '/')[9]	
	raw_filename = str.split(json_filename, '.')[0]
	csv_filename = str.split(json_filename, '.')[0] + '.csv'
	player_gamelog['game.date'] = str.split(raw_filename,'-')[-1]
	gameDate = int(str.split(raw_filename,'-')[-1])
	print('PROCESSING' + str(gameDate))

#created stats for all players
	player_gamelog['player.fullName'] =  player_gamelog['player.firstName'] + ' ' + player_gamelog['player.lastName']
	player_gamelog['player.fullName'] =  player_gamelog['player.fullName'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8') #remove accents from characters
	player_gamelog['player.fullName'] =  player_gamelog['player.fullName'].str.strip().map(msf_player_name.dictionary)

	player_gamelog['opponent.abbreviation'] = np.where(player_gamelog['game.homeTeamAbbreviation'] == player_gamelog['team.abbreviation'], player_gamelog['game.awayTeamAbbreviation'], player_gamelog['game.homeTeamAbbreviation'])
	player_gamelog['game.homeAway'] = np.where(player_gamelog['team.abbreviation'] == player_gamelog['game.homeTeamAbbreviation'], 'H', 'A')


#created batter stats
	player_gamelog['stats.batting.singles'] = player_gamelog['stats.batting.hits'] - player_gamelog['stats.batting.secondBaseHits'] - player_gamelog['stats.batting.thirdBaseHits'] - player_gamelog['stats.batting.homeruns']
	player_gamelog['batting.hand'] = player_gamelog['player.fullName'].map(player_bat_hand.dictionary)

#created pitcher stats
	player_gamelog['stats.pitching.singlesAllowed'] = player_gamelog['stats.pitching.hitsAllowed'] - player_gamelog['stats.pitching.secondBaseHitsAllowed'] - player_gamelog['stats.pitching.thirdBaseHitsAllowed'] - player_gamelog['stats.pitching.homerunsAllowed']


	player_gamelog.loc[(player_gamelog['stats.pitching.completedGames'] == 1) & (player_gamelog['stats.pitching.hitsAllowed'] == 0), 'stats.pitching.noHitter'] = 1
	player_gamelog.loc[(player_gamelog['stats.pitching.completedGames'] != 1) | (player_gamelog['stats.pitching.hitsAllowed'] != 0), 'stats.pitching.noHitter'] = 0


	player_gamelog.loc[(player_gamelog['player.position'] == 'P'), 'player.draftkingsClassicPoints'] = ((player_gamelog['stats.pitching.inningsPitched'] * 2.25) + (player_gamelog['stats.pitching.pitcherStrikeouts'] * 2) + (player_gamelog['stats.pitching.wins'] * 4) 
											+ (player_gamelog['stats.pitching.earnedRunsAllowed'] * -2) + (player_gamelog['stats.pitching.hitsAllowed'] * -0.6) + (player_gamelog['stats.pitching.pitcherWalks'] * -0.6) 
											+ (player_gamelog['stats.pitching.battersHit'] * -0.6) + (player_gamelog['stats.pitching.completedGames'] * 2.5) + (player_gamelog['stats.pitching.shutouts'] * 2.5)
											+ (player_gamelog['stats.pitching.noHitter'] * 5))
	player_gamelog.loc[(player_gamelog['player.position'] != 'P'), 'player.draftkingsClassicPoints'] = ((player_gamelog['stats.batting.singles'] * 3) + (player_gamelog['stats.batting.secondBaseHits'] * 5) + (player_gamelog['stats.batting.thirdBaseHits'] * 8) 
											+ (player_gamelog['stats.batting.homeruns'] * 10) + (player_gamelog['stats.batting.runsBattedIn'] * 2) + (player_gamelog['stats.batting.runs'] * 2) 
											+ (player_gamelog['stats.batting.batterWalks'] * 2) + (player_gamelog['stats.batting.hitByPitch'] * 2) + (player_gamelog['stats.batting.stolenBases'] * 5))


#begin identifying the starting pitcher for each team and each game
	player_gamelog['game.idString'] = player_gamelog['game.id'].astype(str) #casing gameID as a string for concatentation later with the team or opponent.  Used to determine starting pitcher for each game and to deal with double headers
	player_gamelog.loc[(player_gamelog['player.position'] == 'P') & (player_gamelog['stats.miscellaneous.gamesStarted'] == 1), 'game.startingPitcher'] = 1
	player_gamelog['game.startingPitcher'] = player_gamelog['game.startingPitcher'].fillna(0)
	
	player_gamelog.loc[(player_gamelog['game.startingPitcher'] == 1), 'game.startingPitcherName'] = player_gamelog['player.fullName']
	player_gamelog.loc[(player_gamelog['game.startingPitcher'] == 1), 'game.startingPitcherTeam'] = player_gamelog['team.abbreviation']


# batter and pitcher pairing series used to match the game and account for double headers
	player_gamelog.loc[(player_gamelog['game.startingPitcher'] == 1), 'game.startingPitcherGamePairing'] = player_gamelog['game.idString'].str.cat(player_gamelog['game.startingPitcherTeam'])
	player_gamelog['game.batterGamePairing'] = player_gamelog['game.idString'].str.cat(player_gamelog['opponent.abbreviation'])


	player_gamelog['game.startingPitcherName'] = player_gamelog['game.startingPitcherName'].dropna()
	player_gamelog['game.startingPitcherTeam'] = player_gamelog['game.startingPitcherTeam'].dropna()
	player_gamelog['game.startingPitcherGamePairing'] = player_gamelog['game.startingPitcherGamePairing'].dropna()


	starting_pitcher_dict = pd.Series(player_gamelog['game.startingPitcherName'].values, index=player_gamelog['game.startingPitcherGamePairing']).to_dict()
	# pp.pprint(starting_pitcher_dict)
	# pp.pprint(len(starting_pitcher_dict))
	player_gamelog['game.opposingPitcher'] = player_gamelog['game.batterGamePairing'].map(starting_pitcher_dict)
	player_gamelog['game.opposingPitcherHand'] = player_gamelog['game.opposingPitcher'].map(player_throw_hand.dictionary)


#identify missing data
	player_gamelog.loc[(player_gamelog['batting.hand'].isnull()), 'player.missingBatHand'] = player_gamelog['player.fullName']
	player_gamelog.loc[(player_gamelog['game.opposingPitcherHand'].isnull()), 'player.missingThrowHand'] = player_gamelog['player.fullName']
	
	
	#player not in the dictionary
	player_not_in_dictionary = player_gamelog.loc[player_gamelog['player.fullName'].isnull(), ['player.firstName', 'player.lastName']]
	player_not_in_dictionary = player_not_in_dictionary['player.firstName'].map(str) + str(' ') + player_not_in_dictionary['player.lastName'].map(str) + str(' is not in the dictionary')
	if player_not_in_dictionary.count() > 0:
		print(player_not_in_dictionary)


	#player missing batting hand	
	player_missing_batting_hand = player_gamelog.loc[player_gamelog['player.missingBatHand'].notnull(), 'player.fullName']
	if player_missing_batting_hand.count() > 0:
		prLightPurple(player_missing_batting_hand + ' is missing his batting hand')
	
	
	#player missing throwing hand	
	player_missing_throwing_hand = player_gamelog.loc[player_gamelog['player.missingThrowHand'].notnull() & player_gamelog['player.position'] == 'P','player.fullName']
	if player_missing_throwing_hand.count() > 0:
		prCyan(player_missing_throwing_hand + ' is missing his throwing hand')


	#drop unwanted columns
	player_gamelog = player_gamelog.drop(columns = ['game.startingPitcherName','game.startingPitcherTeam', 'game.startingPitcherGamePairing', 'player.missingThrowHand', 'player.missingBatHand'])
	

	#define printing message for terminal output to be isplayed on one line and in color based on success or failure
	def processing_file_message():
		print('Processing ' + (str(gameDate) + '.............'), end="", flush=True)

	#data missing in any columns.  checking after previous line which drops unwanted columns.  Print null data in the DataFrame or report a SUCCESSFUL processing
	null_data = player_gamelog.columns[player_gamelog.isnull().any()]
	print(null_data)

	if null_data.notnull().any():
		processing_file_message(), prRed('FAILURE')
	else:
		processing_file_message(), prGreen('SUCCESS')


	player_gamelog.to_csv(('/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/cleaned_csv/' + '%d' + '-cleaned.csv') % (year, gameDate), index=0, sep=',', encoding='utf-8')
	
	

