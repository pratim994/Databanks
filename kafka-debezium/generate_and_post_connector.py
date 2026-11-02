
import os
import json
import requests
from dotenv import load_dotenv
import logging 

load_dotenv()


connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}


url = "http://localhost:8083/connectors"

headers = {"Content-Type" : "application/json"}
response = requests.post(url, headers=headers, data=json.dumps(connector_config))


if response.status_code == 201:
    print("Connector created successfully")


elif respons.status_code ==  409:
    print("connection already exists")

else:
    print(f"Failed to create connector ({response.status_code}): {response,text}")
