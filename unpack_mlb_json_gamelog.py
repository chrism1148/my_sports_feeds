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
year = 2020
# date = 20160429 # used for single file processing only
path = '/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/raw_json/*json' % (year)  # bulk processing path
# path = '/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/raw_json/daily_player_gamelogs-mlb-%d-regular-%d.json' % (year, year, date)  # single file processing path


for file in glob.glob(path):
	with open(file) as json_gamelog:
		loaded_json = json.load(json_gamelog)
		
		#create player_gamelog DataFrame
		player_gamelog = pd.json_normalize(loaded_json['gamelogs'])

		player_gamelog_float_cols = player_gamelog.select_dtypes(include=['float64']).columns
		player_gamelog_int_cols = player_gamelog.select_dtypes(include=['int']).columns
		player_gamelog_str_cols = player_gamelog.select_dtypes(include=['object']).columns
		
		#fill  missing values
		player_gamelog.loc[:, player_gamelog_float_cols] = player_gamelog.loc[:, player_gamelog_float_cols].fillna(0)
		player_gamelog.loc[:, player_gamelog_int_cols] = player_gamelog.loc[:, player_gamelog_int_cols].fillna(0)
		player_gamelog.loc[:, player_gamelog_str_cols] = player_gamelog.loc[:, player_gamelog_str_cols].fillna('None')

	
#extract strings from filenames for use in creating gameDate series
	json_filename = str.split(file, '/')[9]	
	raw_filename = str.split(json_filename, '.')[0]
	csv_filename = str.split(json_filename, '.')[0] + '.csv'
	player_gamelog['game.date'] = str.split(raw_filename,'-')[-1]
	gameDate = int(str.split(raw_filename,'-')[-1])
	# print('PROCESSING ' + str(gameDate))

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


#IDENTIFY MISSING DATA
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


#DROP UNWANTED COLUMNS
	player_gamelog = player_gamelog.drop(columns = ['player.jerseyNumber', 'game.startingPitcherName','game.startingPitcherTeam', 'game.startingPitcherGamePairing', 'player.missingThrowHand', 'player.missingBatHand'])
	

	#define series dtypes for uniformity across all gamelog dates
	player_gamelog['game.id'] = player_gamelog['game.id'].astype('int64')
	player_gamelog['game.awayTeamAbbreviation'] = player_gamelog['game.awayTeamAbbreviation'].astype('string')
	player_gamelog['game.homeTeamAbbreviation'] = player_gamelog['game.homeTeamAbbreviation'].astype('string')
	player_gamelog['player.id'] = player_gamelog['player.id'].astype('int64')
	player_gamelog['player.firstName'] = player_gamelog['player.firstName'].astype('string')
	player_gamelog['player.lastName'] = player_gamelog['player.lastName'].astype('string')
	player_gamelog['player.position'] = player_gamelog['player.position'].astype('string')
	player_gamelog['team.id'] = player_gamelog['team.id'].astype('int64')
	player_gamelog['team.abbreviation'] = player_gamelog['team.abbreviation'].astype('string')
	player_gamelog['stats.batting.atBats'] = player_gamelog['stats.batting.atBats'].astype('int64')
	player_gamelog['stats.batting.runs'] = player_gamelog['stats.batting.runs'].astype('int64')
	player_gamelog['stats.batting.hits'] = player_gamelog['stats.batting.hits'].astype('int64')
	player_gamelog['stats.batting.secondBaseHits'] = player_gamelog['stats.batting.secondBaseHits'].astype('int64')
	player_gamelog['stats.batting.thirdBaseHits'] = player_gamelog['stats.batting.thirdBaseHits'].astype('int64')
	player_gamelog['stats.batting.homeruns'] = player_gamelog['stats.batting.homeruns'].astype('int64')
	player_gamelog['stats.batting.earnedRuns'] = player_gamelog['stats.batting.earnedRuns'].astype('int64')
	player_gamelog['stats.batting.unearnedRuns'] = player_gamelog['stats.batting.unearnedRuns'].astype('int64')
	player_gamelog['stats.batting.runsBattedIn'] = player_gamelog['stats.batting.runsBattedIn'].astype('int64')
	player_gamelog['stats.batting.batterWalks'] = player_gamelog['stats.batting.batterWalks'].astype('int64')
	player_gamelog['stats.batting.batterSwings'] = player_gamelog['stats.batting.batterSwings'].astype('int64')
	player_gamelog['stats.batting.batterStrikes'] = player_gamelog['stats.batting.batterStrikes'].astype('int64')
	player_gamelog['stats.batting.batterStrikesFoul'] = player_gamelog['stats.batting.batterStrikesFoul'].astype('int64')
	player_gamelog['stats.batting.batterStrikesMiss'] = player_gamelog['stats.batting.batterStrikesMiss'].astype('int64')
	player_gamelog['stats.batting.batterStrikesLooking'] = player_gamelog['stats.batting.batterStrikesLooking'].astype('int64')
	player_gamelog['stats.batting.batterTagOuts'] = player_gamelog['stats.batting.batterTagOuts'].astype('int64')
	player_gamelog['stats.batting.batterForceOuts'] = player_gamelog['stats.batting.batterForceOuts'].astype('int64')
	player_gamelog['stats.batting.batterPutOuts'] = player_gamelog['stats.batting.batterPutOuts'].astype('int64')
	player_gamelog['stats.batting.batterGroundBalls'] = player_gamelog['stats.batting.batterGroundBalls'].astype('int64')
	player_gamelog['stats.batting.batterFlyBalls'] = player_gamelog['stats.batting.batterFlyBalls'].astype('int64')
	player_gamelog['stats.batting.batterLineDrives'] = player_gamelog['stats.batting.batterLineDrives'].astype('int64')
	player_gamelog['stats.batting.batter2SeamFastballs'] = player_gamelog['stats.batting.batter2SeamFastballs'].astype('int64')
	player_gamelog['stats.batting.batter4SeamFastballs'] = player_gamelog['stats.batting.batter4SeamFastballs'].astype('int64')
	player_gamelog['stats.batting.batterCurveballs'] = player_gamelog['stats.batting.batterCurveballs'].astype('int64')
	player_gamelog['stats.batting.batterChangeups'] = player_gamelog['stats.batting.batterChangeups'].astype('int64')
	player_gamelog['stats.batting.batterCutters'] = player_gamelog['stats.batting.batterCutters'].astype('int64')
	player_gamelog['stats.batting.batterSliders'] = player_gamelog['stats.batting.batterSliders'].astype('int64')
	player_gamelog['stats.batting.batterSinkers'] = player_gamelog['stats.batting.batterSinkers'].astype('int64')
	player_gamelog['stats.batting.batterSplitters'] = player_gamelog['stats.batting.batterSplitters'].astype('int64')
	player_gamelog['stats.batting.batterStrikeouts'] = player_gamelog['stats.batting.batterStrikeouts'].astype('int64')
	player_gamelog['stats.batting.stolenBases'] = player_gamelog['stats.batting.stolenBases'].astype('int64')
	player_gamelog['stats.batting.caughtBaseSteals'] = player_gamelog['stats.batting.caughtBaseSteals'].astype('int64')
	player_gamelog['stats.batting.batterStolenBasePct'] = player_gamelog['stats.batting.batterStolenBasePct'].astype('float64')
	player_gamelog['stats.batting.battingAvg'] = player_gamelog['stats.batting.battingAvg'].astype('float64')
	player_gamelog['stats.batting.batterOnBasePct'] = player_gamelog['stats.batting.batterOnBasePct'].astype('float64')
	player_gamelog['stats.batting.batterSluggingPct'] = player_gamelog['stats.batting.batterSluggingPct'].astype('float64')
	player_gamelog['stats.batting.batterOnBasePlusSluggingPct'] = player_gamelog['stats.batting.batterOnBasePlusSluggingPct'].astype('float64')
	player_gamelog['stats.batting.batterIntentionalWalks'] = player_gamelog['stats.batting.batterIntentionalWalks'].astype('int64')
	player_gamelog['stats.batting.hitByPitch'] = player_gamelog['stats.batting.hitByPitch'].astype('int64')
	player_gamelog['stats.batting.batterSacrificeBunts'] = player_gamelog['stats.batting.batterSacrificeBunts'].astype('int64')
	player_gamelog['stats.batting.batterSacrificeFlies'] = player_gamelog['stats.batting.batterSacrificeFlies'].astype('int64')
	player_gamelog['stats.batting.totalBases'] = player_gamelog['stats.batting.totalBases'].astype('int64')
	player_gamelog['stats.batting.extraBaseHits'] = player_gamelog['stats.batting.extraBaseHits'].astype('int64')
	player_gamelog['stats.batting.batterDoublePlays'] = player_gamelog['stats.batting.batterDoublePlays'].astype('int64')
	player_gamelog['stats.batting.batterTriplePlays'] = player_gamelog['stats.batting.batterTriplePlays'].astype('int64')
	player_gamelog['stats.batting.batterGroundOuts'] = player_gamelog['stats.batting.batterGroundOuts'].astype('int64')
	player_gamelog['stats.batting.batterFlyOuts'] = player_gamelog['stats.batting.batterFlyOuts'].astype('int64')
	player_gamelog['stats.batting.batterGroundOutToFlyOutRatio'] = player_gamelog['stats.batting.batterGroundOutToFlyOutRatio'].astype('float64')
	player_gamelog['stats.batting.pitchesFaced'] = player_gamelog['stats.batting.pitchesFaced'].astype('int64')
	player_gamelog['stats.batting.plateAppearances'] = player_gamelog['stats.batting.plateAppearances'].astype('int64')
	player_gamelog['stats.batting.leftOnBase'] = player_gamelog['stats.batting.leftOnBase'].astype('int64')
	player_gamelog['stats.fielding.inningsPlayed'] = player_gamelog['stats.fielding.inningsPlayed'].astype('float64')
	player_gamelog['stats.fielding.totalChances'] = player_gamelog['stats.fielding.totalChances'].astype('int64')
	player_gamelog['stats.fielding.fielderTagOuts'] = player_gamelog['stats.fielding.fielderTagOuts'].astype('int64')
	player_gamelog['stats.fielding.fielderForceOuts'] = player_gamelog['stats.fielding.fielderForceOuts'].astype('int64')
	player_gamelog['stats.fielding.fielderPutOuts'] = player_gamelog['stats.fielding.fielderPutOuts'].astype('int64')
	player_gamelog['stats.fielding.outsFaced'] = player_gamelog['stats.fielding.outsFaced'].astype('int64')
	player_gamelog['stats.fielding.assists'] = player_gamelog['stats.fielding.assists'].astype('int64')
	player_gamelog['stats.fielding.errors'] = player_gamelog['stats.fielding.errors'].astype('int64')
	player_gamelog['stats.fielding.fielderDoublePlays'] = player_gamelog['stats.fielding.fielderDoublePlays'].astype('int64')
	player_gamelog['stats.fielding.fielderTriplePlays'] = player_gamelog['stats.fielding.fielderTriplePlays'].astype('int64')
	player_gamelog['stats.fielding.fielderStolenBasesAllowed'] = player_gamelog['stats.fielding.fielderStolenBasesAllowed'].astype('int64')
	player_gamelog['stats.fielding.fielderCaughtStealing'] = player_gamelog['stats.fielding.fielderCaughtStealing'].astype('int64')
	player_gamelog['stats.fielding.fielderStolenBasePct'] = player_gamelog['stats.fielding.fielderStolenBasePct'].astype('float64')
	player_gamelog['stats.fielding.passedBalls'] = player_gamelog['stats.fielding.passedBalls'].astype('int64')
	player_gamelog['stats.fielding.fielderWildPitches'] = player_gamelog['stats.fielding.fielderWildPitches'].astype('int64')
	player_gamelog['stats.fielding.fieldingPct'] = player_gamelog['stats.fielding.fieldingPct'].astype('float64')
	player_gamelog['stats.fielding.rangeFactor'] = player_gamelog['stats.fielding.rangeFactor'].astype('float64')
	player_gamelog['stats.miscellaneous.gamesStarted'] = player_gamelog['stats.miscellaneous.gamesStarted'].astype('int64')
	player_gamelog['stats.pitching.wins'] = player_gamelog['stats.pitching.wins'].astype('int64')
	player_gamelog['stats.pitching.losses'] = player_gamelog['stats.pitching.losses'].astype('int64')
	player_gamelog['stats.pitching.earnedRunAvg'] = player_gamelog['stats.pitching.earnedRunAvg'].astype('float64')
	player_gamelog['stats.pitching.saves'] = player_gamelog['stats.pitching.saves'].astype('int64')
	player_gamelog['stats.pitching.saveOpportunities'] = player_gamelog['stats.pitching.saveOpportunities'].astype('int64')
	player_gamelog['stats.pitching.inningsPitched'] = player_gamelog['stats.pitching.inningsPitched'].astype('float64')
	player_gamelog['stats.pitching.hitsAllowed'] = player_gamelog['stats.pitching.hitsAllowed'].astype('int64')
	player_gamelog['stats.pitching.secondBaseHitsAllowed'] = player_gamelog['stats.pitching.secondBaseHitsAllowed'].astype('int64')
	player_gamelog['stats.pitching.thirdBaseHitsAllowed'] = player_gamelog['stats.pitching.thirdBaseHitsAllowed'].astype('int64')
	player_gamelog['stats.pitching.runsAllowed'] = player_gamelog['stats.pitching.runsAllowed'].astype('int64')
	player_gamelog['stats.pitching.earnedRunsAllowed'] = player_gamelog['stats.pitching.earnedRunsAllowed'].astype('int64')
	player_gamelog['stats.pitching.homerunsAllowed'] = player_gamelog['stats.pitching.homerunsAllowed'].astype('int64')
	player_gamelog['stats.pitching.pitcherWalks'] = player_gamelog['stats.pitching.pitcherWalks'].astype('int64')
	player_gamelog['stats.pitching.pitcherSwings'] = player_gamelog['stats.pitching.pitcherSwings'].astype('int64')
	player_gamelog['stats.pitching.pitcherStrikes'] = player_gamelog['stats.pitching.pitcherStrikes'].astype('int64')
	player_gamelog['stats.pitching.pitcherStrikesFoul'] = player_gamelog['stats.pitching.pitcherStrikesFoul'].astype('int64')
	player_gamelog['stats.pitching.pitcherStrikesMiss'] = player_gamelog['stats.pitching.pitcherStrikesMiss'].astype('int64')
	player_gamelog['stats.pitching.pitcherStrikesLooking'] = player_gamelog['stats.pitching.pitcherStrikesLooking'].astype('int64')
	player_gamelog['stats.pitching.pitcherGroundBalls'] = player_gamelog['stats.pitching.pitcherGroundBalls'].astype('int64')
	player_gamelog['stats.pitching.pitcherFlyBalls'] = player_gamelog['stats.pitching.pitcherFlyBalls'].astype('int64')
	player_gamelog['stats.pitching.pitcherLineDrives'] = player_gamelog['stats.pitching.pitcherLineDrives'].astype('int64')
	player_gamelog['stats.pitching.pitcher2SeamFastballs'] = player_gamelog['stats.pitching.pitcher2SeamFastballs'].astype('int64')
	player_gamelog['stats.pitching.pitcher4SeamFastballs'] = player_gamelog['stats.pitching.pitcher4SeamFastballs'].astype('int64')
	player_gamelog['stats.pitching.pitcherCurveballs'] = player_gamelog['stats.pitching.pitcherCurveballs'].astype('int64')
	player_gamelog['stats.pitching.pitcherChangeups'] = player_gamelog['stats.pitching.pitcherChangeups'].astype('int64')
	player_gamelog['stats.pitching.pitcherCutters'] = player_gamelog['stats.pitching.pitcherCutters'].astype('int64')
	player_gamelog['stats.pitching.pitcherSliders'] = player_gamelog['stats.pitching.pitcherSliders'].astype('int64')
	player_gamelog['stats.pitching.pitcherSinkers'] = player_gamelog['stats.pitching.pitcherSinkers'].astype('int64')
	player_gamelog['stats.pitching.pitcherSplitters'] = player_gamelog['stats.pitching.pitcherSplitters'].astype('int64')
	player_gamelog['stats.pitching.pitcherSacrificeBunts'] = player_gamelog['stats.pitching.pitcherSacrificeBunts'].astype('int64')
	player_gamelog['stats.pitching.pitcherSacrificeFlies'] = player_gamelog['stats.pitching.pitcherSacrificeFlies'].astype('int64')
	player_gamelog['stats.pitching.pitcherStrikeouts'] = player_gamelog['stats.pitching.pitcherStrikeouts'].astype('int64')
	player_gamelog['stats.pitching.pitchingAvg'] = player_gamelog['stats.pitching.pitchingAvg'].astype('float64')
	player_gamelog['stats.pitching.walksAndHitsPerInningPitched'] = player_gamelog['stats.pitching.walksAndHitsPerInningPitched'].astype('float64')
	player_gamelog['stats.pitching.completedGames'] = player_gamelog['stats.pitching.completedGames'].astype('int64')
	player_gamelog['stats.pitching.shutouts'] = player_gamelog['stats.pitching.shutouts'].astype('int64')
	player_gamelog['stats.pitching.battersHit'] = player_gamelog['stats.pitching.battersHit'].astype('int64')
	player_gamelog['stats.pitching.pitcherIntentionalWalks'] = player_gamelog['stats.pitching.pitcherIntentionalWalks'].astype('int64')
	player_gamelog['stats.pitching.gamesFinished'] = player_gamelog['stats.pitching.gamesFinished'].astype('int64')
	player_gamelog['stats.pitching.holds'] = player_gamelog['stats.pitching.holds'].astype('int64')
	player_gamelog['stats.pitching.pitcherDoublePlays'] = player_gamelog['stats.pitching.pitcherDoublePlays'].astype('int64')
	player_gamelog['stats.pitching.pitcherTriplePlays'] = player_gamelog['stats.pitching.pitcherTriplePlays'].astype('int64')
	player_gamelog['stats.pitching.pitcherGroundOuts'] = player_gamelog['stats.pitching.pitcherGroundOuts'].astype('int64')
	player_gamelog['stats.pitching.pitcherFlyOuts'] = player_gamelog['stats.pitching.pitcherFlyOuts'].astype('int64')
	player_gamelog['stats.pitching.pitcherWildPitches'] = player_gamelog['stats.pitching.pitcherWildPitches'].astype('int64')
	player_gamelog['stats.pitching.balks'] = player_gamelog['stats.pitching.balks'].astype('int64')
	player_gamelog['stats.pitching.pitcherStolenBasesAllowed'] = player_gamelog['stats.pitching.pitcherStolenBasesAllowed'].astype('int64')
	player_gamelog['stats.pitching.pitcherCaughtStealing'] = player_gamelog['stats.pitching.pitcherCaughtStealing'].astype('int64')
	player_gamelog['stats.pitching.pickoffAttempts'] = player_gamelog['stats.pitching.pickoffAttempts'].astype('int64')
	player_gamelog['stats.pitching.pickoffs'] = player_gamelog['stats.pitching.pickoffs'].astype('int64')
	player_gamelog['stats.pitching.totalBattersFaced'] = player_gamelog['stats.pitching.totalBattersFaced'].astype('int64')
	player_gamelog['stats.pitching.pitchesThrown'] = player_gamelog['stats.pitching.pitchesThrown'].astype('int64')
	player_gamelog['stats.pitching.winPct'] = player_gamelog['stats.pitching.winPct'].astype('float64')
	player_gamelog['stats.pitching.pitcherGroundOutToFlyOutRatio'] = player_gamelog['stats.pitching.pitcherGroundOutToFlyOutRatio'].astype('float64')
	player_gamelog['stats.pitching.pitcherOnBasePct'] = player_gamelog['stats.pitching.pitcherOnBasePct'].astype('float64')
	player_gamelog['stats.pitching.pitcherSluggingPct'] = player_gamelog['stats.pitching.pitcherSluggingPct'].astype('float64')
	player_gamelog['stats.pitching.pitcherOnBasePlusSluggingPct'] = player_gamelog['stats.pitching.pitcherOnBasePlusSluggingPct'].astype('float64')
	player_gamelog['stats.pitching.strikeoutsPer9Innings'] = player_gamelog['stats.pitching.strikeoutsPer9Innings'].astype('float64')
	player_gamelog['stats.pitching.walksAllowedPer9Innings'] = player_gamelog['stats.pitching.walksAllowedPer9Innings'].astype('float64')
	player_gamelog['stats.pitching.hitsAllowedPer9Innings'] = player_gamelog['stats.pitching.hitsAllowedPer9Innings'].astype('float64')
	player_gamelog['stats.pitching.strikeoutsToWalksRatio'] = player_gamelog['stats.pitching.strikeoutsToWalksRatio'].astype('float64')
	player_gamelog['stats.pitching.pitchesPerInning'] = player_gamelog['stats.pitching.pitchesPerInning'].astype('float64')
	player_gamelog['stats.pitching.pitcherAtBats'] = player_gamelog['stats.pitching.pitcherAtBats'].astype('int64')
	player_gamelog['game.date'] = player_gamelog['game.date'].astype('int64')
	player_gamelog['player.fullName'] = player_gamelog['player.fullName'].astype('string')
	player_gamelog['opponent.abbreviation'] = player_gamelog['opponent.abbreviation'].astype('string')
	player_gamelog['game.homeAway'] = player_gamelog['game.homeAway'].astype('string')
	player_gamelog['stats.batting.singles'] = player_gamelog['stats.batting.singles'].astype('int64')
	player_gamelog['batting.hand'] = player_gamelog['batting.hand'].astype('string')
	player_gamelog['stats.pitching.singlesAllowed'] = player_gamelog['stats.pitching.singlesAllowed'].astype('int64')
	player_gamelog['stats.pitching.noHitter'] = player_gamelog['stats.pitching.noHitter'].astype('int64')
	player_gamelog['player.draftkingsClassicPoints'] = player_gamelog['player.draftkingsClassicPoints'].astype('float64')
	player_gamelog['game.idString'] = player_gamelog['game.idString'].astype('int64')
	player_gamelog['game.startingPitcher'] = player_gamelog['game.startingPitcher'].astype('int64')
	player_gamelog['game.batterGamePairing'] = player_gamelog['game.batterGamePairing'].astype('string')
	player_gamelog['game.opposingPitcher'] = player_gamelog['game.opposingPitcher'].astype('string')
	player_gamelog['game.opposingPitcherHand'] = player_gamelog['game.opposingPitcherHand'].astype('string')

	

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

	header = [	'game.id',
				'game.startTime',
				'game.awayTeamAbbreviation',
				'game.homeTeamAbbreviation',
				'player.id',
				'player.firstName',
				'player.lastName',
				'player.position',
				'team.id',
				'team.abbreviation',
				'stats.batting.atBats',
				'stats.batting.runs',
				'stats.batting.hits',
				'stats.batting.secondBaseHits',
				'stats.batting.thirdBaseHits',
				'stats.batting.homeruns',
				'stats.batting.earnedRuns',
				'stats.batting.unearnedRuns',
				'stats.batting.runsBattedIn',
				'stats.batting.batterWalks',
				'stats.batting.batterSwings',
				'stats.batting.batterStrikes',
				'stats.batting.batterStrikesFoul',
				'stats.batting.batterStrikesMiss',
				'stats.batting.batterStrikesLooking',
				'stats.batting.batterTagOuts',
				'stats.batting.batterForceOuts',
				'stats.batting.batterPutOuts',
				'stats.batting.batterGroundBalls',
				'stats.batting.batterFlyBalls',
				'stats.batting.batterLineDrives',
				'stats.batting.batter2SeamFastballs',
				'stats.batting.batter4SeamFastballs',
				'stats.batting.batterCurveballs',
				'stats.batting.batterChangeups',
				'stats.batting.batterCutters',
				'stats.batting.batterSliders',
				'stats.batting.batterSinkers',
				'stats.batting.batterSplitters',
				'stats.batting.batterStrikeouts',
				'stats.batting.stolenBases',
				'stats.batting.caughtBaseSteals',
				'stats.batting.batterStolenBasePct',
				'stats.batting.battingAvg',
				'stats.batting.batterOnBasePct',
				'stats.batting.batterSluggingPct',
				'stats.batting.batterOnBasePlusSluggingPct',
				'stats.batting.batterIntentionalWalks',
				'stats.batting.hitByPitch',
				'stats.batting.batterSacrificeBunts',
				'stats.batting.batterSacrificeFlies',
				'stats.batting.totalBases',
				'stats.batting.extraBaseHits',
				'stats.batting.batterDoublePlays',
				'stats.batting.batterTriplePlays',
				'stats.batting.batterGroundOuts',
				'stats.batting.batterFlyOuts',
				'stats.batting.batterGroundOutToFlyOutRatio',
				'stats.batting.pitchesFaced',
				'stats.batting.plateAppearances',
				'stats.batting.leftOnBase',
				'stats.fielding.inningsPlayed',
				'stats.fielding.totalChances',
				'stats.fielding.fielderTagOuts',
				'stats.fielding.fielderForceOuts',
				'stats.fielding.fielderPutOuts',
				'stats.fielding.outsFaced',
				'stats.fielding.assists',
				'stats.fielding.errors',
				'stats.fielding.fielderDoublePlays',
				'stats.fielding.fielderTriplePlays',
				'stats.fielding.fielderStolenBasesAllowed',
				'stats.fielding.fielderCaughtStealing',
				'stats.fielding.fielderStolenBasePct',
				'stats.fielding.passedBalls',
				'stats.fielding.fielderWildPitches',
				'stats.fielding.fieldingPct',
				'stats.fielding.rangeFactor',
				'stats.miscellaneous.gamesStarted',
				'stats.pitching.wins',
				'stats.pitching.losses',
				'stats.pitching.earnedRunAvg',
				'stats.pitching.saves',
				'stats.pitching.saveOpportunities',
				'stats.pitching.inningsPitched',
				'stats.pitching.hitsAllowed',
				'stats.pitching.secondBaseHitsAllowed',
				'stats.pitching.thirdBaseHitsAllowed',
				'stats.pitching.runsAllowed',
				'stats.pitching.earnedRunsAllowed',
				'stats.pitching.homerunsAllowed',
				'stats.pitching.pitcherWalks',
				'stats.pitching.pitcherSwings',
				'stats.pitching.pitcherStrikes',
				'stats.pitching.pitcherStrikesFoul',
				'stats.pitching.pitcherStrikesMiss',
				'stats.pitching.pitcherStrikesLooking',
				'stats.pitching.pitcherGroundBalls',
				'stats.pitching.pitcherFlyBalls',
				'stats.pitching.pitcherLineDrives',
				'stats.pitching.pitcher2SeamFastballs',
				'stats.pitching.pitcher4SeamFastballs',
				'stats.pitching.pitcherCurveballs',
				'stats.pitching.pitcherChangeups',
				'stats.pitching.pitcherCutters',
				'stats.pitching.pitcherSliders',
				'stats.pitching.pitcherSinkers',
				'stats.pitching.pitcherSplitters',
				'stats.pitching.pitcherSacrificeBunts',
				'stats.pitching.pitcherSacrificeFlies',
				'stats.pitching.pitcherStrikeouts',
				'stats.pitching.pitchingAvg',
				'stats.pitching.walksAndHitsPerInningPitched',
				'stats.pitching.completedGames',
				'stats.pitching.shutouts',
				'stats.pitching.battersHit',
				'stats.pitching.pitcherIntentionalWalks',
				'stats.pitching.gamesFinished',
				'stats.pitching.holds',
				'stats.pitching.pitcherDoublePlays',
				'stats.pitching.pitcherTriplePlays',
				'stats.pitching.pitcherGroundOuts',
				'stats.pitching.pitcherFlyOuts',
				'stats.pitching.pitcherWildPitches',
				'stats.pitching.balks',
				'stats.pitching.pitcherStolenBasesAllowed',
				'stats.pitching.pitcherCaughtStealing',
				'stats.pitching.pickoffAttempts',
				'stats.pitching.pickoffs',
				'stats.pitching.totalBattersFaced',
				'stats.pitching.pitchesThrown',
				'stats.pitching.winPct',
				'stats.pitching.pitcherGroundOutToFlyOutRatio',
				'stats.pitching.pitcherOnBasePct',
				'stats.pitching.pitcherSluggingPct',
				'stats.pitching.pitcherOnBasePlusSluggingPct',
				'stats.pitching.strikeoutsPer9Innings',
				'stats.pitching.walksAllowedPer9Innings',
				'stats.pitching.hitsAllowedPer9Innings',
				'stats.pitching.strikeoutsToWalksRatio',
				'stats.pitching.pitchesPerInning',
				'stats.pitching.pitcherAtBats',
				'game.date',
				'player.fullName',
				'opponent.abbreviation',
				'game.homeAway',
				'stats.batting.singles',
				'batting.hand',
				'stats.pitching.singlesAllowed',
				'stats.pitching.noHitter',
				'player.draftkingsClassicPoints',
				'game.idString',
				'game.startingPitcher',
				'game.batterGamePairing',
				'game.opposingPitcher',
				'game.opposingPitcherHand']



	

	player_gamelog.to_csv(('/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/cleaned_csv/' + '%d' + '-cleaned.csv') % (year, gameDate), columns = header, index=0, sep=',', encoding='utf-8')
	
	

