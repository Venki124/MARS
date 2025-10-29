import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.utils.timestamp import Timestamp
import os
import json
import hashlib
import traceback
import logging
import datetime




class ParseMarsPubSubDoFn(beam.DoFn):
    """Parses Pub/Sub messages (supports both JSON and CSV) and routes valid/error outputs."""

    def process(self, message, publish_time=beam.DoFn.TimestampParam):
        try:
            # Decode the Pub/Sub message
            msg = message.decode("utf-8").strip()

            # --- Step 1: Try JSON parse ---
            try:
                payload = json.loads(msg)
                format_type = "json"
            except json.JSONDecodeError:
                # --- Step 2: Fallback to CSV parse ---
                parts = next(csv.reader([msg]))
                if len(parts) != 7:
                    raise ValueError(f"Expected 7 fields in CSV, got {len(parts)}")
                payload = {
                    "timestamp": parts[0].strip(),
                    "ipaddr": parts[1].strip(),
                    "action": parts[2].strip(),
                    "srcacct": parts[3].strip(),
                    "destacct": parts[4].strip(),
                    "amount": parts[5].strip(),
                    "customername": parts[6].strip(),
                }
                format_type = "csv"

            # --- Step 3: Validate required fields ---
            required_fields = ['timestamp', 'ipaddr', 'action', 'srcacct', 'destacct', 'amount', 'customername']
            for f in required_fields:
                if f not in payload or payload[f] in (None, "", "null"):
                    raise ValueError(f"Missing required field: {f}")

            # --- Step 4: Build normalized output row ---
            output_row = {
                "timestamp": str(payload["timestamp"]).strip(),
                "ipaddr": str(payload["ipaddr"]).strip(),
                "action": str(payload["action"]).strip(),
                "srcacct": str(payload["srcacct"]).strip(),
                "destacct": str(payload["destacct"]).strip(),
                "amount": float(payload["amount"]),
                "customername": str(payload["customername"]).strip(),
                "publish_time": str(publish_time.to_utc_datetime()),
                "raw_source": f"pubsub-stream-{format_type}",
                "ingestion_ts": Timestamp.from_utc_datetime(datetime.datetime.now(datetime.timezone.utc))
            }

            # --- Step 5: Add deduplication key ---
            insert_id = hashlib.sha256(
                f"{output_row['timestamp']}-{output_row['srcacct']}-{output_row['action']}".encode()
            ).hexdigest()
            output_row["insert_id"] = insert_id

            yield beam.pvalue.TaggedOutput("valid", output_row)

        except Exception as e:
            # --- Step 6: Send invalid record to error side output ---
            yield beam.pvalue.TaggedOutput("error", {
                "ingestion_ts": Timestamp.from_utc_datetime(datetime.datetime.now(datetime.timezone.utc)),
                "pipeline": "mars-stream",
                "source": "projects/moonbank-mars/topics/activities",
                "payload": msg if 'msg' in locals() else str(message),
                "attributes": None,
                "error_type": "parse_or_validation",
                "error_message": str(e),
                "stacktrace": traceback.format_exc(),
                "retry_count": 0,
                "insert_id": None
            })


def run():


    project = os.getenv('GOOGLE_CLOUD_PROJECT')
    region = 'us-central1'
    bucket = f"{project}-bucket"
    topic = f'projects/{project}/topics/activities-topic'

    job_name = f"mars-stream-job-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

    options = PipelineOptions(
        runner='DataflowRunner',
        project=project,
        region=region,
        job_name=job_name,
        staging_location=f"gs://{bucket}/staging",
        temp_location=f"gs://{bucket}/temp",
        machine_type='e2-standard-2',
        max_num_workers=2,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )
    options.view_as(StandardOptions).streaming = True

    # BigQuery tables
    output_table = f"{project}:mars.activities_stream"
    error_table = f"{project}:mars.stream_ingest_errors"

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(topic=topic)
        )

        parsed = (
            messages
            | 'Parse and Validate' >> beam.ParDo(ParseMarsPubSubDoFn()).with_outputs('valid', 'error')
        )

        valid_rows = parsed['valid']
        error_rows = parsed['error']

        # Write valid records to BigQuery
        valid_rows | 'Write Valid Rows to BQ' >> beam.io.WriteToBigQuery(
            table=output_table,
            schema=(
                'timestamp:STRING, '
                'ipaddr:STRING, '
                'action:STRING, '
                'srcacct:STRING, '
                'destacct:STRING, '
                'amount:FLOAT, '
                'customername:STRING, '
                'publish_time:STRING, '
                'raw_source:STRING, '
                'ingestion_ts:TIMESTAMP, '
                'insert_id:STRING'
            ),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API
        )

        # Write errors to BigQuery error table
        error_rows | 'Write Errors to BQ' >> beam.io.WriteToBigQuery(
            table=error_table,
            schema=(
                'ingestion_ts:TIMESTAMP, '
                'pipeline:STRING, '
                'source:STRING, '
                'payload:STRING, '
                'attributes:STRING, '
                'error_type:STRING, '
                'error_message:STRING, '
                'stacktrace:STRING, '
                'retry_count:INTEGER, '
                'insert_id:STRING'
            ),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API
        )

        logging.info("MARS streaming pipeline is running.")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
