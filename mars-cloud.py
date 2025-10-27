#!/usr/bin/env python3
import apache_beam as beam
import os
import datetime
import util

def processline(line):
    yield line

def run():
    projectname = os.getenv('GOOGLE_CLOUD_PROJECT')
    bucketname = os.getenv('GOOGLE_CLOUD_PROJECT') + '-bucket'
    jobname = 'mars-job' + datetime.datetime.now().strftime("%Y%m%d%H%M")
    region = 'us-central1'
    project_number = util.get_project_number()

    # https://cloud.google.com/dataflow/docs/reference/pipeline-options
    argv = [
      '--runner=DataflowRunner',
      '--project=' + projectname,
      '--job_name=' + jobname,
      '--region=' + region,
      '--staging_location=gs://' + bucketname + '/staging/',
      '--temp_location=gs://' + bucketname + '/temploc/',
      '--max_num_workers=2',
      '--machine_type=e2-standard-2',
      '--service_account_email='+project_number+'-compute@developer.gserviceaccount.com',
      '--save_main_session'
    ]

    p = beam.Pipeline(argv=argv)
    input = 'gs://mars-sample/*.csv'
    # input = 'gs://mars-production/*.csv'
    output = 'gs://' + bucketname + '/output/output'

    (p
     | 'Read Files' >> beam.io.ReadFromText(input)
     | 'Process Lines' >> beam.FlatMap(lambda line: processline(line))
     | 'Write Output' >> beam.io.WriteToText(output)
     )
    p.run()

if __name__ == '__main__':
    run()
