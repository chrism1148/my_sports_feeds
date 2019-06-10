import csv
import pandas as pd
import msf_player_name

game_date = 20190601


####################    DO NOT CHANGE BELOW THIS LINE  ###########################

rotoql_player_ids = pd.read_csv('/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/rotoql_id_%d.csv' % (game_date))
rotoql_player_ids.columns = ['playerRotoQLID', 'playerRotoQLFullName']

rotoql_player_ids['playerFullName'] = rotoql_player_ids['playerRotoQLFullName'].str.strip().map(msf_player_name.dictionary)


header = ['playerRotoQLID', 'playerRotoQLFullName', 'playerFullName']
rotoql_player_ids.to_csv('/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/RotoQL_formatted_ids.csv', sep=',', encoding='utf-8', columns=header, index=0)