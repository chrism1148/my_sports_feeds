import json 
import unidecode
import unicodedata
import numpy as np
import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df
import glob
import os
import player_throw_hand
import player_bat_hand
import pprint
pp = pprint.PrettyPrinter(indent=4)
#load json object
year = 2019
date = 20190608  # used for single file processing only
# path = '/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/%d/game_logs/json/*json' % (year)  # bulk processing path
path = '/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/%d/game_logs/json/daily_player_gamelogs-mlb-2019-regular-%d.json' % (year, date)  # single file processing path


for file in glob.glob(path):
	with open(file) as json_gamelog:
		loaded_json = json.load(json_gamelog)
		player_game_stats = json_normalize(loaded_json['gamelogs'])
		
		float_cols = player_game_stats.select_dtypes(include=['float64']).columns
		int_cols = player_game_stats.select_dtypes(include=['int']).columns
		str_cols = player_game_stats.select_dtypes(include=['object']).columns

		player_game_stats.loc[:, float_cols] = player_game_stats.loc[:, float_cols].fillna(0.0)
		player_game_stats.loc[:, int_cols] = player_game_stats.loc[:, int_cols].fillna(0)
		player_game_stats.loc[:, str_cols] = player_game_stats.loc[:, str_cols].fillna('None')
		player_game_stats.columns = [ 	'gameAwayTeam',
										'gameHomeTeam',
										'gameID',
										'gameStartTime',
										'playerFirstName',
										'playerID',
										'playerJerseyNumber',
										'playerLastName',
										'playerPosition',
										'batterAtBats',
										'batter2SeamFastballs',
										'batter4SeamFastballs',
										'batterChangeups',
										'batterCurveballs',
										'batterCutters',
										'batterDoublePlays',
										'batterFlyBalls',
										'batterFlyOuts',
										'batterForceOuts',
										'batterGroundBalls',
										'batterGroundOutToFlyOutRatio',
										'batterGroundOuts',
										'batterIntentionalWalks',
										'batterLineDrives',
										'batterOnBasePct',
										'batterOnBasePlusSluggingPct',
										'batterPutOuts',
										'batterSacrificeBunts',
										'batterSacrificeFlies',
										'batterSinkers',
										'batterSliders',
										'batterSluggingPct',
										'batterSplitters',
										'batterStolenBasePct',
										'batterStrikeouts',
										'batterStrikes',
										'batterStrikesFoul',
										'batterStrikesLooking',
										'batterStrikesMiss',
										'batterSwings',
										'batterTagOuts',
										'batterTriplePlays',
										'batterWalks',
										'batterAvg',
										'batterCaughtBaseSteals',
										'batterEarnedRuns',
										'batterExtraBaseHits',
										'batterHitByPitch',
										'batterHits',
										'batterHomeruns',
										'batterLeftOnBase',
										'batterPitchesFaced',
										'batterPlateAppearances',
										'batterRuns',
										'batterRunsBattedIn',
										'batterDoubles',
										'batterStolenBases',
										'batterTriples',
										'batterTotalBases',
										'batterUnearnedRuns',
										'fielderAssists',
										'fielderErrors',
										'fielderCaughtStealing',
										'fielderDoublePlays',
										'fielderForceOuts',
										'fielderPutOuts',
										'fielderStolenBasePct',
										'fielderStolenBasesAllowed',
										'fielderTagOuts',
										'fielderTriplePlays',
										'fielderWildPitches',
										'fielderPct',
										'fielderInningsPlayed',
										'fielderOutsFaced',
										'fielderPassedBalls',
										'fielderRangeFactor',
										'fielderTotalChances',
										'playerGamesStarted',
										'pitcherBalks',
										'pitcherBattersHit',
										'pitcherCompletedGames',
										'pitcherEarnedRunAvg',
										'pitcherEarnedRunsAllowed',
										'pitcherGamesFinished',
										'pitcherHitsAllowed',
										'pitcherHitsAllowedPer9Innings',
										'pitcherHolds',
										'pitcherHomerunsAllowed',
										'pitcherInningsPitched',
										'pitcherLosses',
										'pitcherPickoffAttempts',
										'pitcherPickoffs',
										'pitcher2SeamFastballs',
										'pitcher4SeamFastballs',
										'pitcherAtBats',
										'pitcherCaughtStealing',
										'pitcherChangeups',
										'pitcherCurveballs',
										'pitcherCutters',
										'pitcherDoublePlays',
										'pitcherFlyBalls',
										'pitcherFlyOuts',
										'pitcherGroundBalls',
										'pitcherGroundOutToFlyOutRatio',
										'pitcherGroundOuts',
										'pitcherIntentionalWalks',
										'pitcherLineDrives',
										'pitcherOnBasePct',
										'pitcherOnBasePlusSluggingPct',
										'pitcherSacrificeBunts',
										'pitcherSacrificeFlies',
										'pitcherSinkers',
										'pitcherSliders',
										'pitcherSluggingPct',
										'pitcherSplitters',
										'pitcherStolenBasesAllowed',
										'pitcherStrikeouts',
										'pitcherStrikes',
										'pitcherStrikesFoul',
										'pitcherStrikesLooking',
										'pitcherStrikesMiss',
										'pitcherSwings',
										'pitcherTriplePlays',
										'pitcherWalks',
										'pitcherWildPitches',
										'pitcherPitchesPerInning',
										'pitcherPitchesThrown',
										'pitcherPitchingAvg',
										'pitcherRunsAllowed',
										'pitcherSaveOpportunities',
										'pitcherSaves',
										'pitcherDoublesAllowed',
										'pitcherShutouts',
										'pitcherStrikeoutsPer9Innings',
										'pitcherStrikeoutsToWalksRatio',
										'pitcherTriplesAllowed',
										'pitcherTotalBattersFaced',
										'pitcherWalksAllowedPer9Innings',
										'pitcherWalksAndHitsPerInningPitched',
										'pitcherWinPct',
										'pitcherWins',
										'playerTeam',
										'playerTeamID']  


#extract strings from filenames for use in creating gameDate series
		json_filename = str.split(file, '/')[9]	
		raw_filename = str.split(json_filename, '.')[0]
		csv_filename = str.split(json_filename, '.')[0] + '.csv'
		player_game_stats['gameDate'] = str.split(raw_filename,'-')[-1]


#created stats for all players
		player_game_stats['playerFullName'] =  player_game_stats['playerFirstName'] + ' ' + player_game_stats['playerLastName']
		player_game_stats['playerFullName'] =  player_game_stats.playerFullName.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8') #remove accents from characters	

		player_game_stats['playerOpponent'] = np.where(player_game_stats['gameHomeTeam'] == player_game_stats['playerTeam'], player_game_stats['gameAwayTeam'], player_game_stats['gameHomeTeam'])
		player_game_stats['homeAway'] = np.where(player_game_stats['playerTeam'] == player_game_stats['gameHomeTeam'], 'H', 'A')

#created batter stats
		player_game_stats['batterSingles'] = player_game_stats['batterHits'] - player_game_stats['batterDoubles'] - player_game_stats['batterTriples'] - player_game_stats['batterHomeruns']
		player_game_stats['batterHand'] = player_game_stats['playerFullName'].map(player_bat_hand.dictionary)
#created pitcher stats
		player_game_stats['pitcherSinglesAllowed'] = player_game_stats['pitcherHitsAllowed'] - player_game_stats['pitcherDoublesAllowed'] - player_game_stats['pitcherTriplesAllowed'] - player_game_stats['pitcherHomerunsAllowed']


		player_game_stats.loc[(player_game_stats.pitcherCompletedGames == 1) & (player_game_stats.pitcherHitsAllowed == 0), 'pitcherNoHitter'] = 1
		player_game_stats.loc[(player_game_stats.pitcherCompletedGames != 1) | (player_game_stats.pitcherHitsAllowed != 0), 'pitcherNoHitter'] = 0


		player_game_stats.loc[(player_game_stats.playerPosition == 'P'), 'playerDKPoints'] = ((player_game_stats['pitcherInningsPitched'] * 2.25) + (player_game_stats['pitcherStrikeouts'] * 2) + (player_game_stats['pitcherWins'] * 4) 
											+ (player_game_stats['pitcherEarnedRunsAllowed'] * -2) + (player_game_stats['pitcherHitsAllowed'] * -0.6) + (player_game_stats['pitcherWalks'] * -0.6) 
											+ (player_game_stats['pitcherBattersHit'] * -0.6) + (player_game_stats['pitcherCompletedGames'] * 2.5) + (player_game_stats['pitcherShutouts'] * 2.5)
											+ (player_game_stats['pitcherNoHitter'] * 5))
		player_game_stats.loc[(player_game_stats.playerPosition != 'P'), 'playerDKPoints'] = ((player_game_stats['batterSingles'] * 3) + (player_game_stats['batterDoubles'] * 5) + (player_game_stats['batterTriples'] * 8) 
											+ (player_game_stats['batterHomeruns'] * 10) + (player_game_stats['batterRunsBattedIn'] * 2) + (player_game_stats['batterRuns'] * 2) 
											+ (player_game_stats['batterWalks'] * 2) + (player_game_stats['batterHitByPitch'] * 2) + (player_game_stats['batterStolenBases'] * 5))

		player_game_stats.loc[(player_game_stats.playerPosition == 'P') & (player_game_stats.playerGamesStarted == 1), 'startingPitcher'] = 1
		player_game_stats['startingPitcher'] = player_game_stats['startingPitcher'].fillna(0)
		player_game_stats.loc[(player_game_stats.startingPitcher == 1), 'startingPitcherName'] = player_game_stats['playerFullName']
		player_game_stats.loc[(player_game_stats.startingPitcher == 1), 'startingPitcherTeam'] = player_game_stats['playerTeam']

		player_game_stats['startingPitcherName'] = player_game_stats['startingPitcherName'].dropna()
		player_game_stats['startingPitcherTeam'] = player_game_stats['startingPitcherTeam'].dropna()

		starting_pitcher_dict = pd.Series(player_game_stats.startingPitcherName.values, index=player_game_stats.startingPitcherTeam).to_dict()

		player_game_stats['opposingPitcher'] = player_game_stats['playerOpponent'].map(starting_pitcher_dict)
		player_game_stats['opposingPitcherHand'] = player_game_stats['opposingPitcher'].map(player_throw_hand.dictionary)


		player_game_stats.loc[(player_game_stats.batterHand.isnull()), 'missingBatHand'] = player_game_stats['playerFullName']
		player_game_stats.loc[(player_game_stats.opposingPitcherHand.isnull()), 'missingThrowHand'] = player_game_stats['playerFullName']

		print(player_game_stats.missingBatHand.dropna() + " is missing batting hand")
		print(player_game_stats.missingThrowHand.dropna() + " is missing throwing hand")


		header = [	'gameID',
					'gameDate',
					'gameHomeTeam',
					'gameAwayTeam',
					'gameStartTime',
					'playerFullName',
					'playerID',
					'playerJerseyNumber',
					'playerTeam',
					'playerTeamID',
					'homeAway',
					'opposingPitcher',
					'opposingPitcherHand',
					'playerGamesStarted',
					'playerPosition',
					'playerDKPoints',
					'playerOpponent',
					'batterHand',
					'batterPlateAppearances',
					'batterAtBats',
					'batterHits',
					'batterSingles',
					'batterDoubles',
					'batterTriples',
					'batterHomeruns',
					'batterWalks',
					'batterIntentionalWalks',
					'batterHitByPitch',
					'batterRunsBattedIn',
					'batterRuns',
					'batterExtraBaseHits',
					'batterTotalBases',
					'batterStolenBases',
					'batterCaughtBaseSteals',
					'batterEarnedRuns',
					'batterUnearnedRuns',
					'batterOnBasePct',
					'batterAvg',
					'batterSluggingPct',
					'batterOnBasePlusSluggingPct',
					'batterStolenBasePct',
					'batterLineDrives',
					'batterGroundBalls',
					'batterFlyBalls',
					'batterStrikeouts',
					'batterFlyOuts',
					'batterGroundOuts',
					'batterGroundOutToFlyOutRatio',
					'batterForceOuts',
					'batterTagOuts',
					'batterPutOuts',
					'batterDoublePlays',
					'batterTriplePlays',
					'batterSacrificeBunts',
					'batterSacrificeFlies',
					'batterPitchesFaced',
					'batterSwings',
					'batterStrikes',
					'batterStrikesMiss',
					'batterStrikesFoul',
					'batterStrikesLooking',
					'batterLeftOnBase',
					'batter2SeamFastballs',
					'batter4SeamFastballs',
					'batterChangeups',
					'batterCurveballs',
					'batterCutters',
					'batterSinkers',
					'batterSliders',
					'batterSplitters',
					'fielderInningsPlayed',
					'fielderRangeFactor',
					'fielderTotalChances',
					'fielderOutsFaced',
					'fielderPutOuts',
					'fielderForceOuts',
					'fielderTagOuts',
					'fielderAssists',
					'fielderErrors',
					'fielderDoublePlays',
					'fielderTriplePlays',
					'fielderPct',
					'fielderStolenBasePct',
					'fielderStolenBasesAllowed',
					'fielderCaughtStealing',
					'fielderWildPitches',
					'fielderPassedBalls',
					'pitcherInningsPitched',
					'pitcherTotalBattersFaced',
					'pitcherAtBats',
					'pitcherHitsAllowed',
					'pitcherSinglesAllowed',
					'pitcherDoublesAllowed',
					'pitcherTriplesAllowed',
					'pitcherHomerunsAllowed',
					'pitcherWalks',
					'pitcherIntentionalWalks',
					'pitcherBattersHit',
					'pitcherRunsAllowed',
					'pitcherEarnedRunsAllowed',
					'pitcherStolenBasesAllowed',
					'pitcherCaughtStealing',
					'pitcherPitchingAvg',
					'pitcherStrikeouts',
					'pitcherFlyOuts',
					'pitcherGroundOuts',
					'pitcherGroundOutToFlyOutRatio',
					'pitcherDoublePlays',
					'pitcherTriplePlays',
					'pitcherPitchesThrown',
					'pitcher2SeamFastballs',
					'pitcher4SeamFastballs',
					'pitcherChangeups',
					'pitcherCurveballs',
					'pitcherCutters',
					'pitcherSinkers',
					'pitcherSliders',
					'pitcherSplitters',
					'pitcherPitchesPerInning',
					'pitcherStrikes',
					'pitcherSwings',
					'pitcherStrikesMiss',
					'pitcherStrikesLooking',
					'pitcherStrikesFoul',
					'pitcherWildPitches',
					'pitcherBalks',
					'pitcherFlyBalls',
					'pitcherGroundBalls',
					'pitcherLineDrives',
					'pitcherSacrificeFlies',
					'pitcherPickoffAttempts',
					'pitcherPickoffs',
					'pitcherGamesFinished',
					'pitcherCompletedGames',
					'pitcherShutouts',
					'pitcherHolds',
					'pitcherSaveOpportunities',
					'pitcherSaves',
					'pitcherWins',
					'pitcherLosses',
					'pitcherWinPct',
					'pitcherEarnedRunAvg',
					'pitcherHitsAllowedPer9Innings',
					'pitcherStrikeoutsPer9Innings',
					'pitcherWalksAllowedPer9Innings',
					'pitcherStrikeoutsToWalksRatio',
					'pitcherWalksAndHitsPerInningPitched',
					'pitcherOnBasePct',
					'pitcherSluggingPct',
					'pitcherOnBasePlusSluggingPct',
					'pitcherCompletedGames',
					'pitcherNoHitter']
		
		
		player_game_stats.to_csv(('%d/game_logs/csv/' + str(csv_filename)) % (year), sep=',', encoding='utf-8', columns=header, index=0)
		
		print(raw_filename)
