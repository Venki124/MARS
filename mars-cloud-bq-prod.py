import apache_beam as beam
import os
import datetime
import json
import util
from apache_beam.transforms import window
from apache_beam.transforms import trigger

def processline(line):
    yield line

def processbqline(line):
    # Convert CSV line → dictionary
    parts = line.split(',')
    if len(parts) == 7:  # ensure minimum columns
        try:
            outputrow = {
                'timestamp': parts[0].strip(),
                'ipaddr': parts[1].strip(),
                'action': parts[2].strip(),
                'srcacct': parts[3].strip(),
                'destacct': parts[4].strip(),
                'amount': float(parts[5].strip()),
                'customername': parts[6].strip()
            }
            yield outputrow
        except Exception as e:
            # skip invalid lines
            print(f"Skipping line due to error: {e}")
            # outputrow = {
            #     {'errormsg':line,
            #      'error':e
            #     }
            
    else:
        # skip malformed lines
        pass



def run():
    projectname = os.getenv('GOOGLE_CLOUD_PROJECT')
    bucketname = os.getenv('GOOGLE_CLOUD_PROJECT') + '-bucket'
    jobname = 'mars-cloud-bq-job'+datetime.datetime.now().strftime("%Y%m%d%H%M")
    region = 'us-central1'
    project_number = util.get_project_number()

    argv = [
        '--runner=DataflowRunner',
        '--project='+projectname,
        '--job_name='+jobname,
        '--region='+region,
        '--staging_location=gs://'+bucketname+'/staging',
        '--temp_location=gs://'+bucketname+'/temploc/',
        '--machine_type=e2-standard-2',
        '--max_num_workers=6',
        '--service_account_email='+project_number+'-compute@developer.gserviceaccount.com',
        '--save_main_session'
    ]

    p = beam.Pipeline(argv=argv)
    input = 'gs://mars-production/*.csv'
    output = f'gs://{bucketname}/output/output'
    output_table = f'{projectname}:mars.activitiesbq_prod'

    writeoutput = (p
        | 'Read Files' >> beam.io.ReadFromText(input)
        | "process for bq" >> beam.FlatMap(lambda line: processbqline(line))
        # | "window into" >> beam.WindowInto(window.FixedWindows(60),
        #                                    trigger=beam.trigger.AfterProcessingTime(60),
        #                                    accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
        | 'write to BQ' >> beam.io.WriteToBigQuery(table=output_table,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                   schema='timestamp:STRING, ipaddr:STRING, action:STRING, srcacct:STRING, destacct:STRING, amount:FLOAT, customername:STRING',
                                                #    method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS
                                                   method=beam.io.WriteToBigQuery.Method.FILE_LOADS
                                                )
    )

    p.run().wait_until_finish()


if __name__ == '__main__':
    run()