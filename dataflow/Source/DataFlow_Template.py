import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import datetime
import uuid
from google.cloud.proto.datastore.v1 import entity_pb2
from googledatastore import helper as datastore_helper
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore

PROJECT = "gcp-batch-pattern"
BUCKET = 'servian_melb_practice'
DATASET = 'test_batch_servian'
SCHEMA = 'GlobalRank:INTEGER,TldRank:INTEGER,Domain:STRING,TLD:STRING,RefSubNets:INTEGER,RefIPs:INTEGER,IDN_Domain:STRING,' \
         'IDN_TLD:STRING,PrevGlobalRank:INTEGER,PrevTldRank:INTEGER,PrevRefSubNets:INTEGER,PrevRefIPs:INTEGER'
DESCRIPTIONSCHEMA = "TLD_DESC:STRING"
TLD_SCHEMA = 'TLD:STRING,Count:INTEGER'
defaultInputFile = 'gs://{0}/Sample_Data/majestic_million.csv'.format(BUCKET)
TLDFile = 'gs://{0}/Sample_Data/tlds.csv'.format(BUCKET)


def run(argv=None):

    pipeline_args =[
        '--project={0}'.format(PROJECT),
        '--job_name=majesticmillion',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/temp/'.format(BUCKET),
        '--num_workers=4',
        '--runner=DataflowRunner',
        '--inputFile=gs://{0}/Sample_Data/majestic_million.csv'.format(BUCKET),
        #'--template_location=gs://{0}/templates/majestic_million_template'.format(BUCKET),
        '--zone=australia-southeast1-a'
      #  '--region=australia-southeast1',
        ]
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    inbound_options = pipeline_options.view_as(FileLoader)
    input = inbound_options.inputFile

    with beam.Pipeline(options=pipeline_options) as p:
        TLD_Desc =(
            p
            |'Read TLD Description File' >> beam.io.ReadFromText(TLDFile)
            |'Parse Descriptions' >> beam.ParDo(combine_TLD())
            |'Combine Descriptions to Dictionary' >> beam.CombineGlobally(combine_pdict)
            )
        # Extract records as dictionaries
        records =(
            p
            | 'Read File' >> beam.io.ReadFromText(input,skip_header_lines=1)
            | 'Parse CSV' >> beam.ParDo(Split(),SCHEMA)
            | 'Add Descriptions' >> beam.ParDo(AddDTLDDesc(),beam.pvalue.AsSingleton(TLD_Desc))
            )

        # Write TLD aggregations to BigQuery
        (records | 'Aggregate TLDS' >> CountTLDs()
                 | 'Write TLDs to BigQuery' >> beam.io.WriteToBigQuery(
                        '{0}:{1}.TLDCounts'.format(PROJECT, DATASET), # Enter your table name
                        schema=TLD_SCHEMA,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

                )
        )

        # Write all records to BigQuery
        (records
            | 'Write Items BQ' >> beam.io.WriteToBigQuery(
                '{0}:{1}.TopSites'.format(PROJECT, DATASET), # Enter your table name
                schema=SCHEMA+","+DESCRIPTIONSCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )


        # Write metadata to Datastore
        (records
            | 'Get Record Count' >> beam.combiners.Count.Globally()
            | 'Create Metadata' >> beam.ParDo(GetMetaData(inbound_options.inputFile))
            | 'Create DS Entity' >> beam.Map(lambda x : create_ds_entity(x))
            | 'Write To DS' >> WriteToDatastore(PROJECT)
        )

    p.run()

# Class to split the CSV file and format as per the Schema
class Split(beam.DoFn):

    def process(self, element,schema):
        rows = element.split(',')
        header = map(lambda x: x.split(':')[0], schema.split(','))
        return [dict(zip(header, rows))]

class combine_TLD(beam.DoFn):

    def process(self, element):
        rows = element.split(',')
        return [{rows[0]:rows[1]}]


# Class to create an aggregation of TLD's with count
class CountTLDs(beam.PTransform):

    def expand(self, pcoll):
        return (pcoll
            | 'Get TLD from record' >> beam.Map(lambda x: (x['TLD'], 1))
            | 'Aggregate' >> beam.CombinePerKey(beam.combiners.CountCombineFn())
            | 'Count' >> beam.Map(lambda x: {'TLD': x[0], 'Count': x[1]}))


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

class AddDTLDDesc(beam.DoFn):

    def process(self, element,TLDDesc):
        try:
            element['TLD_Desc'] = TLDDesc[element['TLD']]
        except KeyError:
            element['TLD_Desc'] = 'No Description Found'
        #print element
        return [element]


# This creates a datastore entity to be pushed to datastore
def create_ds_entity(element):
    entity = entity_pb2.Entity()
    kind = 'FileMetaData'

    datastore_helper.add_key_path(entity.key,kind,str(uuid.uuid4()))
    datastore_helper.add_properties(entity,element)
    return entity



def combine_pdict(pdict):
    def merge_dict(x, y):
        x.update(y)
        return x
    return reduce(merge_dict,pdict,{})




if __name__ == '__main__':
   run()


