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
date = 20200927 # used for single file processing only
# path = '/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/raw_json/*json' % (year)  # bulk processing path
path = '/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/raw_json/daily_player_gamelogs-mlb-%d-regular-%d.json' % (year, year, date)  # single file processing path


for file in glob.glob(path):
	with open(file) as json_gamelog:
		loaded_json = json.load(json_gamelog)
		
		#create player_attributes DataFrame
		player_attributes = pd.json_normalize(loaded_json['references'])

		for key, values in player_attributes.items():
			if key == 'playerReferences':
				player_references = values

		for key, values in player_references.items():
			player_attributes = pd.DataFrame(values)



		# print(player_references)
		#define column/Series data types
		player_attributes_float_cols = player_attributes.select_dtypes(include=['float64']).columns
		player_attributes_int_cols = player_attributes.select_dtypes(include=['int']).columns
		player_attributes_str_cols = player_attributes.select_dtypes(include=['object']).columns
		#fill  missing values
		player_attributes.loc[:, player_attributes_float_cols] = player_attributes.loc[:, player_attributes_float_cols].fillna(0.0)
		player_attributes.loc[:, player_attributes_int_cols] = player_attributes.loc[:, player_attributes_int_cols].fillna(0)
		player_attributes.loc[:, player_attributes_str_cols] = player_attributes.loc[:, player_attributes_str_cols].fillna('None')

		player_attributes['fullName'] =  player_attributes['firstName'] + ' ' + player_attributes['lastName']
		player_attributes['fullName'] =  player_attributes['fullName'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8') #remove accents from characters
		# player_gamelog['player.fullName'] =  player_gamelog['player.fullName'].str.strip().map(msf_player_name.dictionary)

		player_hand = player_attributes[['fullName', 'handedness']]

		players_missing_hands = [	
									'Tucker Davidson',
									'Demarcus Evans',
									'Matt Foster',
									'Deivi Garcia',
									'Nick Heath',
									'Carlos Hernandez',
									'Yadiel Hernandez',
									'James Kaprielian',
									'Alejandro Kirk',
									'Vimael Machin',
									'Jared Oliva',
									'Edward Olivares',
									'Andre Scrubb',
									'Jordan Weems',
									'Miguel Yajure',
									'Tyler Zuber',
									'Sergio Alcantara',
									'Jonathan Arauz',
									'Wes Benjamin',
									'JT Brubaker',
									'Blake Cederlind',
									'Josh Fleming',
									'Ashton Goudeau',
									'Codi Heuer',
									'Cam Hill',
									'Mike Kickham',
									'Brailyn Marquez',
									'Nick Nelson',
									'Nivaldo Rodriguez',
									'Tarik Skubal',
									'Riley Smith',
									'Justin Topa',
									'Daz Cameron']



		player_hand = player_hand.loc[player_hand['fullName'].isin(players_missing_hands)]

		print(player_hand)

		player_hand_output = player_hand.to_csv('/Users/chrismccallan/documents/statis/mlb/player_references/references3.csv', sep=',', encoding='utf-8')
	

