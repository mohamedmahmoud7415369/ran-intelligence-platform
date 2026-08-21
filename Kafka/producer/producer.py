import csv
import json
import os
import re
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer


# ============================================================
# Project Paths
# ============================================================

DATA_DIR_ENV = os.getenv("DATA_DIR")

if DATA_DIR_ENV:
    DATA_DIR = Path(DATA_DIR_ENV)
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DATA_DIR = PROJECT_ROOT / "data" / "Raw"

# ============================================================
# Kafka Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1:9092,kafka2:9092,kafka3:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ran-telemetry"
)


# ============================================================
# Producer Runtime Configuration
# ============================================================

SEND_DELAY_SECONDS = float(
    os.getenv("SEND_DELAY_SECONDS", "0")
)

MAX_EVENTS = int(
    os.getenv("MAX_EVENTS", "0")
)

SOURCE_FILE = os.getenv(
    "SOURCE_FILE", ""
)


# ============================================================
# Filename Pattern
#
# Examples:
#
# Dataset_01_LTE_2100.csv
# Dataset_03_NR_3500.csv
# Dataset_03_GSM_900.csv
#
# ============================================================

FILENAME_PATTERN = re.compile(
    r"^Dataset_(\d+)_(LTE|NR|GSM)_(\d+)\.csv$"
)


# ============================================================
# Common CSV Column Mapping
# ============================================================

COMMON_COLUMN_MAP = {
    "Base station": "base_station",
    "Sector": "sector",
    "Timestamp": "event_timestamp",

    "Radio unit energy consumption":
        "radio_unit_energy_consumption",

    "Baseband energy consumption":
        "baseband_energy_consumption",
}


# ============================================================
# LTE / 4G Mapping
# ============================================================

LTE_COLUMN_MAP = {
    "4G max active users DL": "max_active_users_dl",
    "4G max active users UL": "max_active_users_ul",

    "4G data volume DL": "data_volume_dl",
    "4G data volume UL": "data_volume_ul",

    "4G max RRC users": "max_rrc_users",
    "4G RRC users": "rrc_users",

    "4G RB utilization": "rb_utilization",

    "4G CQI rank 1": "cqi_rank_1",
    "4G CQI rank 2": "cqi_rank_2",
    "4G CQI rank 3": "cqi_rank_3",
    "4G CQI rank 4": "cqi_rank_4",

    "4G active users UL": "active_users_ul",
    "4G active users DL": "active_users_dl",

    "4G MIMO rank DL": "mimo_rank_dl",
}


# ============================================================
# NR / 5G Mapping
# ============================================================

NR_COLUMN_MAP = {
    "5G max active users DL": "max_active_users_dl",
    "5G max active users UL": "max_active_users_ul",

    "5G data volume DL": "data_volume_dl",
    "5G data volume UL": "data_volume_ul",

    "5G max RRC users": "max_rrc_users",
    "5G RRC users": "rrc_users",

    "5G RB utilization": "rb_utilization",

    "5G CQI rank 1": "cqi_rank_1",
    "5G CQI rank 2": "cqi_rank_2",
    "5G CQI rank 3": "cqi_rank_3",
    "5G CQI rank 4": "cqi_rank_4",

    "5G active users UL": "active_users_ul",
    "5G active users DL": "active_users_dl",

    "5G MIMO rank DL": "mimo_rank_dl",
}


# ============================================================
# GSM / 2G Mapping
# ============================================================

GSM_COLUMN_MAP = {
    "2G TS available": "ts_available",
    "2G TS used": "ts_used",
    "2G TS utilization": "ts_utilization",
}


TECHNOLOGY_COLUMN_MAPS = {
    "LTE": LTE_COLUMN_MAP,
    "NR": NR_COLUMN_MAP,
    "GSM": GSM_COLUMN_MAP,
}


# ============================================================
# Parse Metadata From Filename
# ============================================================

def parse_filename_metadata(file_path):
    filename = Path(file_path).name

    match = FILENAME_PATTERN.match(filename)

    if not match:
        raise ValueError(
            f"Unexpected filename format: {filename}"
        )

    dataset_number = match.group(1)
    technology = match.group(2)
    frequency_mhz = int(match.group(3))

    return {
        "dataset_id": f"Dataset_{dataset_number}",
        "technology": technology,
        "frequency_mhz": frequency_mhz,
        "source_file": filename,
    }


# ============================================================
# Build Common Column Mapping
# ============================================================

def get_column_mapping(technology):
    if technology not in TECHNOLOGY_COLUMN_MAPS:
        raise ValueError(
            f"Unsupported technology: {technology}"
        )

    return {
        **COMMON_COLUMN_MAP,
        **TECHNOLOGY_COLUMN_MAPS[technology],
    }


# ============================================================
# Validate CSV Schema
#
# This does NOT clean the data.
# It only checks that the CSV structure is expected.
# ============================================================

def validate_csv_schema(fieldnames, technology):
    if not fieldnames:
        raise ValueError("CSV file has no header")

    expected_mapping = get_column_mapping(technology)

    expected_columns = set(expected_mapping.keys())
    actual_columns = set(fieldnames)

    missing_columns = expected_columns - actual_columns
    unknown_columns = actual_columns - expected_columns

    if missing_columns:
        raise ValueError(
            f"Missing columns for {technology}: "
            f"{sorted(missing_columns)}"
        )

    if unknown_columns:
        raise ValueError(
            f"Unexpected columns for {technology}: "
            f"{sorted(unknown_columns)}"
        )


# ============================================================
# Normalize Column Names
#
# IMPORTANT:
# We only rename column names.
#
# Values stay exactly as they came from CSV.
#
# Examples:
#
# ""      stays ""
# "33.44" stays "33.44"
# "0"     stays "0"
# timestamp stays "10/23/2023 14:30"
#
# Cleaning happens later in Spark.
# ============================================================

def normalize_row(raw_row, technology):
    column_mapping = get_column_mapping(technology)

    normalized = {}

    for raw_column, raw_value in raw_row.items():
        normalized_column = column_mapping[raw_column]

        normalized[normalized_column] = raw_value

    return normalized


# ============================================================
# Build Cell ID
#
# No aggressive cleaning / slug generation here.
#
# Example:
#
# Site 36 | 2 | LTE | 2100
#
# becomes:
#
# Site 36|2|LTE|2100
#
# ============================================================

def build_cell_id(
    base_station,
    sector,
    technology,
    frequency_mhz
):
    if not base_station:
        raise ValueError(
            "Base station is empty"
        )

    if not sector:
        raise ValueError(
            "Sector is empty"
        )

    return (
        f"{base_station}|"
        f"{sector}|"
        f"{technology}|"
        f"{frequency_mhz}"
    )


# ============================================================
# Build Kafka Event
# ============================================================

def build_event(raw_row, metadata):
    technology = metadata["technology"]

    normalized_row = normalize_row(
        raw_row,
        technology
    )

    cell_id = build_cell_id(
        base_station=normalized_row["base_station"],
        sector=normalized_row["sector"],
        technology=technology,
        frequency_mhz=metadata["frequency_mhz"],
    )

    event = {
        # Event metadata
        "event_id": str(uuid.uuid4()),

        "ingestion_timestamp":
            datetime.now(timezone.utc).isoformat(),

        "schema_version": "1.0",

        # Source metadata
        "dataset_id": metadata["dataset_id"],
        "source_file": metadata["source_file"],

        # RAN metadata
        "technology": metadata["technology"],
        "frequency_mhz": metadata["frequency_mhz"],

        # Logical cell identifier
        "cell_id": cell_id,

        # Original measurement fields
        **normalized_row,
    }

    return event


# ============================================================
# Kafka Delivery Callback
# ============================================================

def delivery_report(err, msg):
    if err is not None:
        print(
            f"[DELIVERY FAILED] "
            f"topic={msg.topic()} "
            f"error={err}"
        )

    else:
        print(
            f"[DELIVERED] "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"key={msg.key().decode('utf-8')}"
        )


# ============================================================
# Create Kafka Producer
# ============================================================

def create_kafka_producer():
    config = {
        "bootstrap.servers":
            KAFKA_BOOTSTRAP_SERVERS,

        "acks":
            "all",

        "enable.idempotence":
            True,

        "client.id":
            "ran-telemetry-producer",

        "linger.ms":
            10,

        "batch.num.messages":
            1000,

        "message.timeout.ms":
            300000,
    }

    return Producer(config)


# ============================================================
# Send One Event
# ============================================================

def send_event(producer, event):
    key = event["cell_id"]

    value = json.dumps(
        event,
        ensure_ascii=False
    ).encode("utf-8")

    while True:
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=key.encode("utf-8"),
                value=value,
                callback=delivery_report,
            )

            break

        except BufferError:
            print(
                "[WARNING] Producer queue full. "
                "Waiting for Kafka..."
            )

            producer.poll(1.0)

    producer.poll(0)


# ============================================================
# Produce One CSV File
# ============================================================

def produce_file(
    producer,
    file_path,
    remaining_limit=None
):
    metadata = parse_filename_metadata(
        file_path
    )

    print()
    print("=" * 70)
    print(f"Reading: {metadata['source_file']}")
    print(
        f"Technology: "
        f"{metadata['technology']}"
    )
    print(
        f"Frequency: "
        f"{metadata['frequency_mhz']} MHz"
    )
    print("=" * 70)

    sent_count = 0

    with open(
        file_path,
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        validate_csv_schema(
            reader.fieldnames,
            metadata["technology"]
        )

        for row_number, raw_row in enumerate(
            reader,
            start=2
        ):
            try:
                event = build_event(
                    raw_row,
                    metadata
                )

                send_event(
                    producer,
                    event
                )

                sent_count += 1

                if SEND_DELAY_SECONDS > 0:
                    time.sleep(
                        SEND_DELAY_SECONDS
                    )

                if (
                    remaining_limit is not None
                    and sent_count >= remaining_limit
                ):
                    break

            except Exception as exc:
                print(
                    f"[ROW ERROR] "
                    f"file={metadata['source_file']} "
                    f"row={row_number} "
                    f"error={exc}"
                )

    return sent_count


# ============================================================
# Discover CSV Files
# ============================================================

def discover_files():
    if SOURCE_FILE:
        source_path = Path(SOURCE_FILE)

        if not source_path.is_absolute():
            source_path = DATA_DIR / source_path

        if not source_path.exists():
            raise FileNotFoundError(
                f"Source file not found: "
                f"{source_path}"
            )

        return [source_path]

    files = sorted(
        DATA_DIR.glob("Dataset_*.csv")
    )

    if not files:
        raise FileNotFoundError(
            f"No CSV files found in: "
            f"{DATA_DIR}"
        )

    return files


# ============================================================
# Main
# ============================================================

def main():
    print()
    print("RAN Telemetry Kafka Producer")
    print("=" * 70)

    print(
        f"Kafka brokers : "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Kafka topic   : "
        f"{KAFKA_TOPIC}"
    )

    print(
        f"Data directory: "
        f"{DATA_DIR}"
    )

    print(
        f"Send delay    : "
        f"{SEND_DELAY_SECONDS} seconds"
    )

    if MAX_EVENTS > 0:
        print(
            f"Max events    : "
            f"{MAX_EVENTS}"
        )
    else:
        print(
            "Max events    : unlimited"
        )

    producer = create_kafka_producer()

    files = discover_files()

    print()
    print(
        f"Found {len(files)} CSV file(s)"
    )

    total_sent = 0

    try:
        for file_path in files:

            remaining_limit = None

            if MAX_EVENTS > 0:
                remaining_limit = (
                    MAX_EVENTS - total_sent
                )

                if remaining_limit <= 0:
                    break

            sent_from_file = produce_file(
                producer,
                file_path,
                remaining_limit
            )

            total_sent += sent_from_file

    except KeyboardInterrupt:
        print()
        print(
            "[STOP] Producer interrupted by user"
        )

    finally:
        print()
        print(
            "Waiting for pending Kafka messages..."
        )

        remaining = producer.flush(30)

        print()
        print("=" * 70)

        print(
            f"Total events sent: "
            f"{total_sent}"
        )

        if remaining == 0:
            print(
                "All Kafka messages delivered."
            )
        else:
            print(
                f"{remaining} message(s) "
                f"were not delivered."
            )

        print("=" * 70)


if __name__ == "__main__":
    main()