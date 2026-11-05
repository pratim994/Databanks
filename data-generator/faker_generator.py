
import time
import psycopg2
from decimal import Decimal, ROUND_DOWN
#postgresql adapter for python 
import argparse
import sys
import os
import random
from faker import Faker

from dotenv import load_dotenv


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT_DIR, ".env"))


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
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


conn = psycopg2.connect(
    host= os.getenv("POSTGRES_HOST", "localhost"),
    port = os.getenv("POSTGRES_PORT", "5433"),
    dbname = os.getenv("POSTGRES_DB"),
    user =os.getenv("POSTGRES_USER"), 
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


    accounts = []
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
            cur.execute(
                    "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id",
                    (customer_id, account_type, initial_balance, CURRENCY),
                )

            account_id = cur.fetchone()[0]
            accounts.append(account_id)



    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(1, MAX_TXN_AMOUNT),2)
        related_account =  None

        if txn_type == "TRANSFER" and len(accounts) > 1:
            related_account = random.choice([a for a in accounts if a != account_id])


        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) VALUES (%s, %s, %s, %s, 'COMPLETED')",
            (account_id, txn_type, amount, related_account),
                )
    
    print(f"✅ Generated {len(customers)} customers, {len(accounts)} accounts, {NUM_TRANSACTIONS} transactions.")    




try:
    iteration = 0

    while True:
        iteration += 1
        print(f"running {iteration} iteration")

        run_iteration()


        print(f"Iteration {iteration} finished")

        if not LOOP:
            break

        time.sleep(SLEEP_SECONDS)

except KeyboardInterrupt:
    print("\n Interrupted by user :( ")


finally:
    cur.close()
    conn.close()
