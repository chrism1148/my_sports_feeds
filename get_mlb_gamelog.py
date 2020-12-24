from ohmysportsfeedspy import MySportsFeeds
import datetime


msf = MySportsFeeds(version='2.1',verbose=True)
msf.authenticate("API KEY", "MYSPORTSFEEDS")


# Pull CSV from My Sports Feeds API
year = 2016
days = [datetime.datetime(year, 4, 3) + datetime.timedelta(days=i) for i in range(183)]
game_date = [day.strftime("%Y%m%d") for day in days]


for day in game_date:
	output = msf.msf_get_data(league='mlb',season=str(year)+'-regular',feed='daily_player_gamelogs' ,format='json', date=day, fordate=day)
