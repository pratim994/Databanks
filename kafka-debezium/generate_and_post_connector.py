import os
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

required = [
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
]

missing = [key for key in required if not os.getenv(key)]
if missing:
    raise RuntimeError(f"Missing environment variables: {missing}")

connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        # Debezium runs inside Docker, so use Docker DNS and port.
        "database.hostname": os.getenv("DEBEZIUM_DB_HOST", "postgres"),
        "database.port": "5432",
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),

        "topic.prefix": "banking_server",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.name": "banking_publication",
        "publication.autocreate.mode": "filtered",

        "table.include.list": (
            "public.customers,"
            "public.accounts,"
            "public.transactions"
        ),

        "snapshot.mode": "initial",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}

url = "http://localhost:8083/connectors"

try:
    response = requests.post(
        url,
        json=connector_config,
        timeout=30
    )

    print("HTTP status:", response.status_code)
    print("Response:", response.text)

    if response.status_code == 201:
        print("Connector created successfully.")
    elif response.status_code == 409:
        print("Connector already exists. Check its status.")
    else:
        response.raise_for_status()

except requests.RequestException as error:
    print("Connector registration failed:", error)
    raise
