import os

"""Get runtime environment variables"""
dataset_id = os.environ.get('dataset_id')
table_name = os.environ.get('table_name')
bucket_name = os.environ.get('bucket_name')
source_file_name = os.environ.get('source_file_name')

def import_to_big_query(data, context, dataset=dataset_id, table=table_name, verbose=True):
    def vprint(s):
        if verbose:
            print(s)

    vprint('Event ID: {}'.format(context.event_id))
    vprint('Event type: {}'.format(context.event_type))
    vprint('Importing required modules.')

    from google.cloud import bigquery

    vprint('This is the data: {}'.format(data))

    input_bucket_name = bucket_name
    source_file = source_file_name
    uri = 'gs://{}/{}'.format(input_bucket_name, source_file)

    vprint('Getting the data from bucket "{}"'.format(
        uri
    ))
    
    if str(source_file).lower().endswith('.csv') or \
            str(source_file).lower().endswith('.avro'):

        client = bigquery.Client()
        dataset_ref = client.dataset(dataset)

        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True

        if str(source_file).lower().endswith('.csv'):
            job_config.source_format = bigquery.SourceFormat.CSV
        else:
            job_config.source_format = bigquery.SourceFormat.AVRO

        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        load_job = client.load_table_from_uri(
            uri,
            dataset_ref.table(table),
            job_config=job_config)

        vprint('Starting job {}'.format(load_job.job_id))

        load_job.result()
        vprint('Job finished.')

        destination_table = client.get_table(dataset_ref.table(table))
        vprint('Loaded {} rows.'.format(destination_table.num_rows))

        vprint('File imported successfully.')
    else:
        vprint('Not an importable file.')
