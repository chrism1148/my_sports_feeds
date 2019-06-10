import os
import glob

from google.cloud import bigquery



os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/GCP-BQ.json"

year = 2019
gamelog_date = 20190608  # used for single file processing only
projections_date = 20190609

# gamelog_path = '/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/%d/game_logs/csv/*.csv' % (year)  # bulk file uploading
gamelog_path = '/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/%d/game_logs/csv/daily_player_gamelogs-mlb-2019-regular-%d.csv' % (year, gamelog_date)  # single file uploading
projections_path = '/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/%d/player_projections/dk_mlb_projections_%d.csv' % (year, projections_date)


client = bigquery.Client()
dataset_id = 'my_sports_feeds'

# uplaod gamelogs
gamelog_job_config = bigquery.LoadJobConfig()
gamelog_job_config.source_format = bigquery.SourceFormat.CSV
gamelog_job_config.skip_leading_rows = 1
gamelog_job_config.autodetect = True
gamelog_job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

for filename in glob.glob(gamelog_path):
	json_filename = str.split(filename, '/')[9]	
	raw_filename = str.split(json_filename, '.')[0]
	raw_filename = str.split(raw_filename,'-')[-1]
	file_date = ''.join(i for i in raw_filename if i.isdigit())
	table_id = 'mlb_player_gamelog_' + str(file_date)
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
projections_job_config = bigquery.LoadJobConfig()
projections_job_config.source_format = bigquery.SourceFormat.CSV
projections_job_config.skip_leading_rows = 1
projections_job_config.autodetect = True

for filename in glob.glob(projections_path):
	json_filename = str.split(filename, '/')[8]	
	raw_filename = str.split(json_filename, '.')[0]
	raw_filename = str.split(raw_filename,'_')[-1]
	file_date = ''.join(i for i in raw_filename if i.isdigit())

	table_id = 'mlb_player_projections_' + str(file_date)
	dataset_ref = client.dataset(dataset_id)
	table_ref = dataset_ref.table(table_id)
	with open(filename, 'rb') as source_file:
		job = client.load_table_from_file(
        source_file,
        table_ref,
        location='US',  # Must match the destination dataset location.
        job_config=projections_job_config)  # API request

		job.result()  # Waits for table load to complete.

		print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))