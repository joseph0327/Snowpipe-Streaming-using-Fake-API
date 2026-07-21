

# This script simulates a bank system that generates transactions from multiple producers 
# (MOBILE_APP, WEB_APP, ATM_SYSTEM) and streams them into Snowflake using the Streaming 
# Ingest API. Each producer generates transactions with random attributes such as merchant, 
# transaction type, amount, currency, status, channel, country, device ID, and reference number.
# The transactions are batched and flushed to Snowflake at regular intervals.

from snowflake.ingest.streaming import StreamingIngestClient
import uuid
import time
import random
from datetime import datetime

PRODUCERS=["MOBILE_APP","WEB_APP","ATM_SYSTEM"]

DATABASE="TRANSACTIONSBD"
SCHEMA="TRANSACTION_SCHEMA"
TABLE="TRANSACTIONS_TABLE_STREAMING"
PIPE = f"{TABLE}-STREAMING"

BATCH_SIZE=10
FLUSH_INTERVAL=1

MERCHANTS=["Amazon","Tesco","Uber","Netflix","Deliveroo","Apple","ASOS","Spotify","Starbucks"]
CURRENCIES=["GBP","USD","EUR"]
STATUSES=["APPROVED","APPROVED","APPROVED","DECLINED","PENDING"]
TRANSACTION_TYPES=["PURCHASE","TRANSFER","PAYMENT","WITHDRAWAL"]
CHANNEL_TYPES=["MOBILE","WEB","ATM"]
COUNTRIES=["UK","USA","Germany","France","Singapore"]

def make_transaction(seq,producer):
    return {
        "transaction_id":str(uuid.uuid4()),
        "sequence":seq,
        "source":producer,
        "customer_id":f"CUST{random.randint(100000,999999)}",
        "account_id":f"ACC{random.randint(10000,99999)}",
        "merchant":random.choice(MERCHANTS),
        "transaction_type":random.choice(TRANSACTION_TYPES),
        "amount":round(random.uniform(10,5000),2),
        "currency":random.choice(CURRENCIES),
        "status":random.choice(STATUSES),
        "channel":random.choice(CHANNEL_TYPES),
        "country":random.choice(COUNTRIES),
        "device_id":f"DEVICE-{random.randint(1000,9999)}",
        "reference_number":f"REF-{uuid.uuid4().hex[:10].upper()}",
        "event_time":datetime.utcnow()
    }

client_name=f"BANK_SYSTEM_{uuid.uuid4()}"

with StreamingIngestClient(
    client_name=client_name,
    db_name=DATABASE,
    schema_name=SCHEMA,
    pipe_name=PIPE,
    profile_json="profile.json"
) as client:

    channels={}
    sequences={}
    buffers={}

    for producer in PRODUCERS:
        channel_name=f"CHANNEL_{producer}"
        channel,status=client.open_channel(channel_name)
        channels[producer]=channel
        last_offset=status.latest_committed_offset_token
        sequences[producer]=int(last_offset)+1 if last_offset else 0
        buffers[producer]=[]
        print(producer,"starting from sequence",sequences[producer])

    while True:
        for producer in PRODUCERS:

            sequence=sequences[producer]
            transaction=make_transaction(
                sequence,
                producer
            )

            channel=channels[producer]
            channel.append_row(
                transaction,
                str(sequence)
            )

            buffers[producer].append(transaction)
            sequences[producer]+=1

            print(
                producer,
                "created transaction",
                sequence,
                transaction["transaction_type"],
                transaction["amount"],
                transaction["currency"]
            )

            if len(buffers[producer])>=BATCH_SIZE:

                print(
                    "Flushing",
                    producer,
                    len(buffers[producer]),
                    "rows"
                )

                channel.initiate_flush()
                channel.wait_for_flush()

                buffers[producer].clear()

        time.sleep(FLUSH_INTERVAL)


