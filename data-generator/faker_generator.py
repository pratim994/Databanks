
import time
import psycopg2
#postgresql adapter for python 
import argparse
import sys
import os

from dotenv import load_dotenv


load_dotenv()


NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TXN_AMOUNT = 1000.00
CURRENCY = "USD"

INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")

DEFAULT_LOOP = True
SLEEP_SECONDS = 2


parser = argparse.ArgumentParser(description="Run fake data generator ")
parser.add_argument("--once", action="store_true",help="Run a single interaction and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

fake = Faker()

def random_money(min_val: Decimal, max_val: Decimal ) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01", rounding=ROUND_DOWN))


conn = pyscopg2.connect(
    host= os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    dbname = os.getenv("POSTGRES_DB")
    user =os.getenv("POSTGRES_USER") 
    password = os.getenv("POSTGRES_PASSWORD")
  )

conn.autocommit = True
cur = conn.cursor()



def run_iteration():
    customers = []

    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()

        cur.execute(
                "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
                (first_name, last_name, email),
        )
        customer_id = cur.fetchone()[0]
        customers.append(customer_id)


