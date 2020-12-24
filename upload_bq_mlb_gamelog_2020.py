import os
import glob

from google.cloud import bigquery



os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/chrismccallan/documents/statis/mlb/scripts/GCP-BQ.json"

year = 2020
# gamelog_date = 20160429 # used for single file processing only
# projections_date = 20190906

gamelog_path = '/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/cleaned_csv/*.csv' % (year)  # bulk file uploading
# gamelog_path = '/Users/chrismccallan/documents/statis/mlb/gamelogs/%d/cleaned_csv/%d-cleaned.csv' % (year, gamelog_date)  # single file uploading
# projections_path = '/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/%d/player_projections/dk_mlb_projections_%d.csv' % (year, projections_date)


client = bigquery.Client()
dataset_id = 'MLB'

# uplaod gamelogs
gamelog_job_config = bigquery.LoadJobConfig(
	schema=[
		bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("game_startTime", "TIMESTAMP", mode="REQUIRED"),
		bigquery.SchemaField("game_awayTeamAbbreviation", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("game_homeTeamAbbreviation", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("player_id", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("player_firstName", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("player_lastName", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("player_position", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("team_id", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("team_abbreviation", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_atBats", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_runs", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_hits", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_secondBaseHits", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_thirdBaseHits", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_homeruns", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_earnedRuns", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_unearnedRuns", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_runsBattedIn", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterWalks", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterSwings", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterStrikes", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterStrikesFoul", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterStrikesMiss", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterStrikesLooking", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterTagOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterForceOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterPutOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterGroundBalls", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterFlyBalls", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterLineDrives", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batter2SeamFastballs", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batter4SeamFastballs", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterCurveballs", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterChangeups", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterCutters", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterSliders", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterSinkers", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterSplitters", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterStrikeouts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_stolenBases", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_caughtBaseSteals", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterStolenBasePct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_battingAvg", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterOnBasePct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterSluggingPct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterOnBasePlusSluggingPct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterIntentionalWalks", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_hitByPitch", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterSacrificeBunts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterSacrificeFlies", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_totalBases", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_extraBaseHits", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterDoublePlays", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterTriplePlays", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterGroundOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterFlyOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_batterGroundOutToFlyOutRatio", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_pitchesFaced", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_plateAppearances", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_leftOnBase", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_inningsPlayed", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_totalChances", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fielderTagOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fielderForceOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fielderPutOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_outsFaced", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_assists", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_errors", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fielderDoublePlays", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fielderTriplePlays", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fielderStolenBasesAllowed", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fielderCaughtStealing", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fielderStolenBasePct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_passedBalls", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fielderWildPitches", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_fieldingPct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_fielding_rangeFactor", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_miscellaneous_gamesStarted", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_wins", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_losses", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_earnedRunAvg", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_saves", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_saveOpportunities", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_inningsPitched", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_hitsAllowed", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_secondBaseHitsAllowed", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_thirdBaseHitsAllowed", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_runsAllowed", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_earnedRunsAllowed", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_homerunsAllowed", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherWalks", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherSwings", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherStrikes", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherStrikesFoul", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherStrikesMiss", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherStrikesLooking", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherGroundBalls", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherFlyBalls", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherLineDrives", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcher2SeamFastballs", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcher4SeamFastballs", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherCurveballs", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherChangeups", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherCutters", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherSliders", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherSinkers", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherSplitters", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherSacrificeBunts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherSacrificeFlies", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherStrikeouts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitchingAvg", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_walksAndHitsPerInningPitched", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_completedGames", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_shutouts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_battersHit", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherIntentionalWalks", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_gamesFinished", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_holds", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherDoublePlays", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherTriplePlays", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherGroundOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherFlyOuts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherWildPitches", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_balks", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherStolenBasesAllowed", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherCaughtStealing", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pickoffAttempts", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pickoffs", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_totalBattersFaced", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitchesThrown", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_winPct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherGroundOutToFlyOutRatio", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherOnBasePct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherSluggingPct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherOnBasePlusSluggingPct", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_strikeoutsPer9Innings", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_walksAllowedPer9Innings", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_hitsAllowedPer9Innings", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_strikeoutsToWalksRatio", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitchesPerInning", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_pitcherAtBats", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("game_date", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("player_fullName", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("opponent_abbreviation", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("game_homeAway", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("stats_batting_singles", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("batting_hand", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_singlesAllowed", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("stats_pitching_noHitter", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("player_draftkingsClassicPoints", "FLOAT", mode="REQUIRED"),
		bigquery.SchemaField("game_idString", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("game_startingPitcher", "INTEGER", mode="REQUIRED"),
		bigquery.SchemaField("game_batterGamePairing", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("game_opposingPitcher", "STRING", mode="REQUIRED"),
		bigquery.SchemaField("game_opposingPitcherHand", "STRING", mode="REQUIRED"),],

		source_format = bigquery.SourceFormat.CSV,
		skip_leading_rows = 1,
		autodetect = True,
		write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)



for filename in glob.glob(gamelog_path):
	print('processing ' + str(filename))
	raw_filename = str.split(filename,'/')[-1]
	file_date = str.split(raw_filename,'-')[0]
	table_id = 'gamelog_' + str(file_date)
	dataset_ref = client.dataset(dataset_id)
	table_ref = dataset_ref.table(table_id)
	with open(filename, 'rb') as source_file:
		job = client.load_table_from_file(
        source_file,
        table_ref,

        location='US',  # Must match the destination dataset location.
        job_config=gamelog_job_config)  # API request

		job.result()  # Waits for table load to complete.

		print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))


# upload projections
# projections_job_config = bigquery.LoadJobConfig()
# projections_job_config.source_format = bigquery.SourceFormat.CSV
# projections_job_config.skip_leading_rows = 1
# projections_job_config.autodetect = True
# projections_job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

# for filename in glob.glob(projections_path):
# 	json_filename = str.split(filename, '/')[8]	
# 	raw_filename = str.split(json_filename, '.')[0]
# 	raw_filename = str.split(raw_filename,'_')[-1]
# 	file_date = ''.join(i for i in raw_filename if i.isdigit())

# 	table_id = 'mlb_player_projections_' + str(file_date)
# 	dataset_ref = client.dataset(dataset_id)
# 	table_ref = dataset_ref.table(table_id)
# 	with open(filename, 'rb') as source_file:
# 		job = client.load_table_from_file(
#         source_file,
#         table_ref,
#         location='US',  # Must match the destination dataset location.
#         job_config=projections_job_config)  # API request

# 		job.result()  # Waits for table load to complete.

# 		print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))