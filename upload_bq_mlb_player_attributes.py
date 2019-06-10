import os
import glob

from google.cloud import bigquery

year = 2018
path = '/Users/chrismccallan/desktop/ohmysportsfeedspy/mlb/%d/players/csv/*.csv' % (year)


client = bigquery.Client()
dataset_id = 'my_sports_feeds'

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.autodetect = True

for filename in glob.glob(path):
	json_filename = str.split(filename, '/')[9]	
	raw_filename = str.split(json_filename, '.')[0]
	raw_filename = str.split(raw_filename,'-')[-1]
	file_date = ''.join(i for i in raw_filename if i.isdigit())
	table_id = 'mlb_player_attributes_' + str(file_date)
	dataset_ref = client.dataset(dataset_id)
	table_ref = dataset_ref.table(table_id)
	with open(filename, 'rb') as source_file:
		job = client.load_table_from_file(
        source_file,
        table_ref,
        location='US',  # Must match the destination dataset location.
        job_config=job_config)  # API request

		job.result()  # Waits for table load to complete.

		print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))