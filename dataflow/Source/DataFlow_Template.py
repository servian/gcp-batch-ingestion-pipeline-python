import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
from apache_beam.options.pipeline_options import SetupOptions
import datetime
import uuid


from google.cloud.proto.datastore.v1 import entity_pb2
from googledatastore import helper as datastore_helper
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore



# Setting up project variables
PROJECT = "your_project_id"
BUCKET = 'your_GCS_bucket_name'
DATASET = 'your_dataset_name'
SCHEMA = 'File_Schema in the format of Column_Name:Data_Type,.. '
defaultInputFile = 'gs://{0}/*.csv'.format(BUCKET)


# Setting up the Environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'Service_Account json file path'



# Class to split the CSV file and format as per the Schema

class Split(beam.DoFn):

    def process(self, element):
        rows = element.split(',')
        header = map(lambda x: x.split(':')[0], SCHEMA.split(','))
        return [dict(zip(header, rows))]


# Class to create metadata of loaded file
class GetMetaData(beam.DoFn):

    def __init__(self, fileName):
        self.fileName = fileName

    def process(self, element):
        rec_count = element
        proc_time = datetime.datetime.now()
        f_name = self.fileName.get()

        return [{'FileName':unicode(f_name), 'RecordCount':rec_count,'LoadTimeStamp':proc_time}]


# Define additional command line input to get full path name
class FileLoader(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--inputFile',
                                           dest='inputFile',
                                           default=defaultInputFile,
                                           help='Input file to process'
                                           )


# This creates a datastore entity to be pushed to datastore
def create_ds_entity(element):
    entity = entity_pb2.Entity()
    kind = 'FileMetaData'

    datastore_helper.add_key_path(entity.key,kind,str(uuid.uuid4()))
    datastore_helper.add_properties(entity,element)
    return entity



# Main run method
def run(argv=None):

    pipeline_args =[
        '--project={0}'.format(PROJECT),
        '--job_name=gcs2bqsample5',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/temp/'.format(BUCKET),
        '--runner=DataflowRunner',
        '--template_location=gs://{0}/templates/majestic_million_template'.format(BUCKET), #Dataflow template name
        '--zone=australia-southeast1-a'
      #  '--region=australia-southeast1',
        ]


    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    inbound_options = pipeline_options.view_as(FileLoader)
    input = inbound_options.inputFile

    with beam.Pipeline(options=pipeline_options) as p:


        # Extract records as dictionaries
        records =(
            p
            | 'Read File' >> beam.io.ReadFromText(input,skip_header_lines=1)
            | 'Parse CSV' >> beam.ParDo(Split())
        )

        # Write data to BigQuery
        (
            records
            | 'Write BQ' >> beam.io.WriteToBigQuery
             (
                '{0}:{1}.Majestic_Web_Data'.format(PROJECT, DATASET), # Enter your table name
                schema=SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
             )
        )


        # Write metadata to Datastore
        (
            records
            | 'Get Record Count' >> beam.combiners.Count.Globally()
            | 'Create Metadata' >> beam.ParDo(GetMetaData(inbound_options.inputFile))
            | 'Create DS Entity' >> beam.Map(lambda x : create_ds_entity(x))
            | 'Write To DS' >> WriteToDatastore(PROJECT)
        )

    p.run()


if __name__ == '__main__':
   run()


