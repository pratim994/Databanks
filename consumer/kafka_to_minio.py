import os
import json
import uuid
import tempfile
import logging
import time

from datetime import datetime, timezone

import boto3
import pandas as pd

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
from dotenv import load_dotenv


from pathlib import Path

ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_FILE)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

TOPICS = [
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions"
]

BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "10"))
FLUSH_INTERVAL_SECONDS = int(os.getenv("KAFKA_FLUSH_INTERVAL_SECONDS", "5"))

BUCKET = os.getenv("MINIO_BUCKET", "raw")


# Kafka configuration
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=os.getenv(
        "KAFKA_BOOTSTRAP", "localhost:29092"
    ),
    group_id=os.getenv(
        "KAFKA_GROUP", "banking-minio-consumer"
    ),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: (
        json.loads(x.decode("utf-8")) if x else None
    )
)


s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER")),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD")),
    region_name="us-east-1"
)


# Create bucket if it does not exist
existing_buckets = [
    bucket["Name"]
    for bucket in s3.list_buckets()["Buckets"]
]

if BUCKET not in existing_buckets:
    s3.create_bucket(Bucket=BUCKET)
    logging.info("Created MinIO bucket: %s", BUCKET)


# One independent buffer per Kafka topic partition
buffers = {}


def upload_to_minio(topic, records):

    if not records:
        return

    table_name = topic.split(".")[-1]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    file_name = f"{uuid.uuid4().hex}.parquet"

    s3_key = (
        f"{table_name}/date={date_str}/{file_name}"
    )

    df = pd.DataFrame(records)

    temp_file = None

    try:
        with tempfile.NamedTemporaryFile(
            suffix=".parquet",
            delete=False
        ) as temp:
            temp_file = temp.name

        df.to_parquet(
            temp_file,
            engine="pyarrow",
            index=False
        )

        s3.upload_file(
            temp_file,
            BUCKET,
            s3_key
        )

        logging.info(
            "Uploaded %s records to s3://%s/%s",
            len(records),
            BUCKET,
            s3_key
        )

    finally:
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)


def process_message(message):

    event = message.value

    # Ignore Kafka tombstone messages
    if event is None:
        return None

    # Debezium JSON with schemas enabled uses payload
    payload = event.get("payload", event)

    operation = payload.get("op")

    # Inserts and updates use after.
    # Deletes use before because after is null.
    data = payload.get("after") or payload.get("before")

    if not data:
        return None

    record = dict(data)

    # Preserve CDC metadata
    record["_operation"] = operation
    record["_source_table"] = message.topic.split(".")[-1]
    record["_kafka_topic"] = message.topic
    record["_kafka_partition"] = message.partition
    record["_kafka_offset"] = message.offset
    record["_event_timestamp"] = payload.get("ts_ms")

    return record


def commit_partition(partition, offset):

    consumer.commit({
        partition: OffsetAndMetadata(offset + 1, "")
    })


def flush_partition(partition):

    batch = buffers.get(partition)

    if not batch or not batch["records"]:
        return

    # Upload first. Commit only after successful upload.
    upload_to_minio(
        batch["topic"],
        batch["records"]
    )

    commit_partition(
        partition,
        batch["last_offset"]
    )

    logging.info(
        "Committed %s offset %s",
        partition,
        batch["last_offset"]
    )

    batch["records"].clear()
    batch["last_offset"] = None


logging.info("Connected. Listening for Kafka CDC events...")


try:
    last_flush = time.monotonic()
    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=500)
        for messages in polled.values():
            for message in messages:
                partition = TopicPartition(message.topic, message.partition)

                if partition not in buffers:
                    buffers[partition] = {
                        "topic": message.topic,
                        "records": [],
                        "last_offset": None
                    }

                record = process_message(message)

                # Tombstones and empty events carry no row data.
                if record is None:
                    # Never commit past earlier buffered records in the same partition.
                    flush_partition(partition)
                    commit_partition(partition, message.offset)
                    continue

                batch = buffers[partition]
                batch["records"].append(record)
                batch["last_offset"] = message.offset

                if len(batch["records"]) >= BATCH_SIZE:
                    flush_partition(partition)

        if time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
            for partition in list(buffers.keys()):
                flush_partition(partition)
            last_flush = time.monotonic()


except KeyboardInterrupt:

    logging.info("Shutdown requested. Flushing remaining records...")


finally:

    try:
        for partition in list(buffers.keys()):
            flush_partition(partition)

    finally:
        consumer.close()
        logging.info("Kafka consumer stopped.")
