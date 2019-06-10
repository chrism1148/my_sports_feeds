import json 
import unidecode
import unicodedata
from flatten_json import flatten
import numpy as np
import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df
import glob
import os


#load json object
year = 2016
path = '/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/%d/players/json/*.json' % (year)


for file in glob.glob(path):
	with open(file) as json_gamelog:
		loaded_json = json.load(json_gamelog)
		player_attributes = json_normalize(loaded_json['players'])

		float_cols = player_attributes.select_dtypes(include=['float64']).columns
		int_cols = player_attributes.select_dtypes(include=['int']).columns
		str_cols = player_attributes.select_dtypes(include=['object']).columns

		player_attributes.loc[:, float_cols] = player_attributes.loc[:, float_cols].fillna(0.0)
		player_attributes.loc[:, int_cols] = player_attributes.loc[:, int_cols].fillna(0)
		player_attributes.loc[:, str_cols] = player_attributes.loc[:, str_cols].fillna('None')
		player_attributes.columns = [ 				'playerAge',
													'playerAlternatePositions',
													'playerBirthCity',
													'playerBirthCountry',
													'playerBirthDate',
													'playerCollege',
													'playerCurrentContractYear',
													'playerBaseSalary',
													'playerCapHit',
													'playerFullNoTradeClause',
													'playerMinorsSalary',
													'playerModifiedNoTradeClause',
													'playerNoMovementClause',
													'playerOtherBonuses',
													'playerAnnualAverageSalary',
													'playerExpiryStatus',
													'playerSignedOn',
													'playerSigningTeam',
													'playerSigningTeamID',
													'playerTotalBonuses',
													'playerTotalSalary',
													'playerTotalYears',
													'playerSeasonStartYear',
													'playerSigningBonus',
													'playerCurrentInjury',
													'playerCurrentInjuryDescription',
													'playerPlayingProbability',
													'playerCurrentRosterStatus',
													'playerCurrentTeamFull',
													'playerCurrentTeam',
													'playerCurrentTeamID',
													'playerDrafted',
													'playerDraftedOverallPick',
													'playerDraftedPickTeam',
													'playerDraftedPickTeamID',
													'playerDraftedRound',
													'playerDraftedRoundPick',
													'playerDraftedTeam',
													'playerDraftedTeamID',
													'playerDraftedYear',
													'playerExternalMappings',
													'playerFirstName',
													'playerHandednessBats',
													'playerHandednessThrows',
													'playerHeight',
													'playerHighSchool',
													'playerID',
													'playerJerseyNumber',
													'playerLastName',
													'playerOfficialImageSrc',
													'playerPrimaryPosition',
													'playerRookie',
													'playerSocialMediaAccounts',
													'playerWeight',
													'playerTeamAsOfDateFull',
													'playerTeamAsOfDate',
													'playerTeamAsOfDateID',]  

			#created attributes for all players
		player_attributes['playerFullName'] =  player_attributes['playerFirstName'] + ' ' + player_attributes['playerLastName']
		player_attributes['playerFullName'] =  player_attributes.playerFullName.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8') #remove accents from characters	

		json_filename = str.split(file, '/')[9]	
		raw_filename = str.split(json_filename, '.')[0]
		csv_filename = str.split(json_filename, '.')[0] + '.csv'

		player_attributes['gameDate'] = str.split(raw_filename,'-')[-1]

		header = [									'gameDate',
													'playerFullName',
													'playerID',
													'playerAge',										
													'playerPrimaryPosition',
													'playerAlternatePositions',
													'playerTeamAsOfDate',
													'playerTeamAsOfDateID',
													'playerJerseyNumber',
													'playerBirthCity',
													'playerBirthCountry',
													'playerBirthDate',
													'playerCollege',
													'playerCurrentContractYear',
													'playerBaseSalary',
													'playerCapHit',
													'playerFullNoTradeClause',
													'playerMinorsSalary',
													'playerModifiedNoTradeClause',
													'playerNoMovementClause',
													'playerOtherBonuses',
													'playerAnnualAverageSalary',
													'playerExpiryStatus',
													'playerSignedOn',
													'playerSigningTeam',
													'playerSigningTeamID',
													'playerTotalBonuses',
													'playerTotalSalary',
													'playerTotalYears',
													'playerSeasonStartYear',
													'playerSigningBonus',
													'playerCurrentInjury',
													'playerCurrentInjuryDescription',
													'playerPlayingProbability',
													'playerCurrentRosterStatus',
													'playerCurrentTeamFull',
													'playerCurrentTeam',
													'playerCurrentTeamID',
													'playerDrafted',
													'playerDraftedOverallPick',
													'playerDraftedPickTeam',
													'playerDraftedPickTeamID',
													'playerDraftedRound',
													'playerDraftedRoundPick',
													'playerDraftedTeam',
													'playerDraftedTeamID',
													'playerDraftedYear',
													'playerExternalMappings',
													'playerFirstName',
													'playerHandednessBats',
													'playerHandednessThrows',
													'playerHeight',
													'playerHighSchool',
													'playerLastName',
													'playerOfficialImageSrc',
													'playerPrimaryPosition',
													'playerRookie',
													'playerSocialMediaAccounts',
													'playerWeight',
													'playerCurrentTeam',
													'playerCurrentTeamID',
			]
		
		
		player_attributes.to_csv(('%d/players/csv/' + str(csv_filename)) % (year), sep=',', columns=header, encoding='utf-8',  index=0)
		print(raw_filename)

		# player_attributes.to_csv('%d/players/csv/players_%d.csv' % (year, game_date), sep=',', columns=header, encoding='utf-8',  index=0)
		# print(str(game_date) + ' completed')
		# game_date = game_date + 1