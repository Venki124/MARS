import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import datetime
import csv
import hashlib
import logging
import traceback
from datetime import datetime as dt


class ParseMarsCsvDoFn(beam.DoFn):
    """Parses CSV lines and emits valid + error outputs."""

    def process(self, line, file_name=beam.DoFn.ElementParam):
        try:
            parts = next(csv.reader([line]))
            if len(parts) != 7:
                raise ValueError(f"Expected 7 fields, got {len(parts)}")

            output_row = {
                'timestamp': parts[0].strip(),
                'ipaddr': parts[1].strip(),
                'action': parts[2].strip(),
                'srcacct': parts[3].strip(),
                'destacct': parts[4].strip(),
                'amount': float(parts[5].strip()),
                'customername': parts[6].strip()
            }

            # Basic validation
            if not output_row['action']:
                raise ValueError("Missing action field")

            # Generate a stable insert ID
            insert_id = hashlib.sha256(
                f"{output_row['timestamp']}-{output_row['srcacct']}".encode()
            ).hexdigest()

            output_row['insert_id'] = insert_id
            yield beam.pvalue.TaggedOutput('valid', output_row)

        except Exception as e:
            logging.error(f"Error parsing line: {e}")
            yield beam.pvalue.TaggedOutput('error', {
                'ingestion_ts': dt.now(datetime.timezone.utc).isoformat(),
                'pipeline': 'mars-batch',
                'source': 'gcs',
                'payload': line,
                'attributes': None,
                'error_type': 'parse_error',
                'error_message': str(e),
                'stacktrace': traceback.format_exc(),
                'retry_count': 0,
                'insert_id': None
            })


def run():
    project = os.getenv('GOOGLE_CLOUD_PROJECT')
    bucket = f"{project}-bucket"
    region = "us-central1"
    job_name = f"mars-batch-job-{datetime.datetime.now().strftime('%Y%m%d%H%M')}"

    options = PipelineOptions(
        runner='DataflowRunner',
        project=project,
        job_name=job_name,
        region=region,
        staging_location=f"gs://{bucket}/staging",
        temp_location=f"gs://{bucket}/temp",
        machine_type='e2-standard-2',
        max_num_workers=2,
        save_main_session=True
    )

    p = beam.Pipeline(options=options)

    input_pattern = "gs://moonbank-mars-sample/*.csv"
    output_table = f"{project}:mars.activities_batch"
    error_table = f"{project}:mars.batch_ingest_errors"

    parsed = (
        p
        | 'Read CSV files' >> beam.io.ReadFromText(input_pattern, skip_header_lines=0)
        | 'Parse CSV lines' >> beam.ParDo(ParseMarsCsvDoFn()).with_outputs('valid', 'error')
    )

    valid = parsed['valid']
    errors = parsed['error']

    valid | 'Write valid rows to BQ' >> beam.io.WriteToBigQuery(
        table=output_table,
        schema='timestamp:STRING, ipaddr:STRING, action:STRING, srcacct:STRING, destacct:STRING, amount:FLOAT, customername:STRING, insert_id:STRING',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.FILE_LOADS
    )

    errors | 'Write error rows to BQ' >> beam.io.WriteToBigQuery(
        table=error_table,
        schema='ingestion_ts:TIMESTAMP,pipeline:STRING,source:STRING,payload:STRING,attributes:STRING,error_type:STRING,error_message:STRING,stacktrace:STRING,retry_count:INTEGER,insert_id:STRING',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.FILE_LOADS
    )

    p.run().wait_until_finish()
    logging.info("MARS batch pipeline completed successfully.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
