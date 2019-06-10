import csv
import re
import unicodedata
import sys
import os
import pandas as pd
import player_throw_hand
import msf_player_name

game_date = 20190609


####################    DO NOT CHANGE BELOW THIS LINE  ###########################

gameday_players = pd.read_csv('/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/2019/player_projections/dk_mlb_projections_%d_raw.csv' % (game_date))
gameday_players.columns = ['playerFullName', 'playerTeam', 'playerOpponent', 'playerPosition', 'playerDKSalary', 'myProjection', 'blank', 'rotoQLProjection', 'dfsGuruProjection', 'last5GamesAvg', 'seasonAvg']



gameday_players['gameDate'] = game_date
gameday_players.loc[(gameday_players.playerTeam.str.contains('@')), 'playerHomeAway'] = 'H'
gameday_players.loc[(gameday_players.playerOpponent.str.contains('@')), 'playerHomeAway'] = 'A'
gameday_players['playerFullName'] = gameday_players['playerFullName'].str.strip(to_strip=' Q ')

# gameday_players['playerFullName'] = gameday_players['playerFullName'].str.strip(to_strip=' O ')
gameday_players['playerFullName'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
gameday_players['playerFullName'] = gameday_players['playerFullName'].str.strip().map(msf_player_name.dictionary)
gameday_players['playerTeam'] = gameday_players['playerTeam'].str.strip(to_strip='@')
gameday_players['playerOpponent'] = gameday_players['playerOpponent'].str.strip(to_strip='@')

gameday_players['playerTeam'] = gameday_players['playerTeam'].str.replace('WSH', 'WAS')
gameday_players['playerOpponent'] = gameday_players['playerOpponent'].str.replace('WSH', 'WAS')

gameday_players['playerThrowHand'] = gameday_players['playerFullName'].map(player_throw_hand.dictionary)



header = ['gameDate', 'playerFullName', 'playerThrowHand', 'playerTeam', 'playerOpponent', 'playerHomeAway', 'playerPosition', 'playerDKSalary', 'rotoQLProjection', 'dfsGuruProjection', 'last5GamesAvg', 'seasonAvg']

gameday_players.to_csv('/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/2019/player_projections/dk_mlb_projections_%d.csv' % (game_date), sep=',', encoding='utf-8', columns=header, index=0)