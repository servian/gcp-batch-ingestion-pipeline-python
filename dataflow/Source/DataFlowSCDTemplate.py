import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import datetime
import hashlib
import json

PROJECT = "gcp-batch-pattern"
BUCKET = 'servian_melb_practice'
DATASET = 'test_batch_servian_scd'
SCHEMA = 'GlobalRank:INTEGER,TldRank:INTEGER,Domain:STRING,TLD:STRING,RefSubNets:INTEGER,RefIPs:INTEGER,IDN_Domain:STRING,' \
         'IDN_TLD:STRING,PrevGlobalRank:INTEGER,PrevTldRank:INTEGER,PrevRefSubNets:INTEGER,PrevRefIPs:INTEGER,MD5:STRING,StartDate:TIMESTAMP,EndDate:TIMESTAMP'
defaultInputFile = 'gs://{0}/Sample_Data/majestic_thousand_staging.csv'.format(BUCKET)

WRITE_IN_DATETIME = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def run(argv=None):
    logging.basicConfig(level=logging.INFO)

    pipeline_args = [
        '--project={0}'.format(PROJECT),
        '--job_name=majesticmillion-staging',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/temp/'.format(BUCKET),
        '--num_workers=4',
        '--runner=DataflowRunner',
        # '--template_location=gs://{0}/templates/majestic_million_template'.format(BUCKET),
        '--zone=australia-southeast1-a'
        #  '--region=australia-southeast1',
    ]
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    inbound_options = pipeline_options.view_as(FileLoader)
    input = inbound_options.inputFile

    with beam.Pipeline(options=pipeline_options) as p:
        # Extract records as dictionaries
        records = (
                p
                | 'Read File' >> beam.io.ReadFromText(input, skip_header_lines=1)
                | 'Parse CSV' >> beam.ParDo(Split()))

        # Write all records to BigQuery
        (records
         | 'Write Items BQ' >> beam.io.WriteToBigQuery(
                    '{0}:{1}.TopSites_Staging'.format(PROJECT, DATASET),  # Enter your table name
                    schema=SCHEMA,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                )
         )

    p.run()


# Class to split the CSV file and format as per the Schema
class Split(beam.DoFn):

    def process(self, element):
        row = element.split(',')
        data_md5 = hashlib.md5(json.dumps(row, sort_keys=True)).hexdigest()
        row.extend([data_md5, WRITE_IN_DATETIME])
        logging.info(row)
        header = map(lambda x: x.split(':')[0], SCHEMA.split(','))
        return [dict(zip(header, row))]


# Define additional command line input to get full path name
class FileLoader(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--inputFile',
                                           dest='inputFile',
                                           default=defaultInputFile,
                                           help='Input file to process'
                                           )


if __name__ == '__main__':
    run()
