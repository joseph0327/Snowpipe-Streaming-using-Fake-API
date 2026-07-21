"""
Simulates a continuous stream of financial transactions into Snowflake
using the Snowpipe Streaming SDK.

Generates synthetic transactions every 6 seconds, appends them to a
persistent Snowflake streaming channel, and flushes records for near
real-time ingestion. Offset tracking allows the stream to resume safely
after restarts without duplicate events.

GENERIC template for streaming.
"""

from datetime import datetime
import time
import uuid
import os
import random

os.environ["SS_LOG_LEVEL"] = "warn"

from snowflake.ingest.streaming import StreamingIngestClient

DATABASE = "TRANSACTIONSBD" 
SCHEMA = "TRANSACTION_SCHEMA"
TABLE = "TRANSACTIONS_TABLE"
PIPE = f"{TABLE}-STREAMING"

# 10 transactions per minute = 1 every 6 seconds
INTERVAL_SECONDS = 6

MERCHANTS = ["Amazon", "Tesco", "Uber", "Netflix", "Deliveroo", "Apple", "ASOS", "Spotify"]
CURRENCIES = ["GBP", "USD", "EUR"]
STATUSES = ["approved", "approved", "approved", "declined", "pending"]


def main():
    print(f"Starting transaction stream - {60 // INTERVAL_SECONDS} tx/min")
    print("Press Ctrl+C to stop.\n")

    # Use a stable channel name so offset is preserved across restarts
    channel_name = "TRANSACTIONS_SIMULATOR"

    with StreamingIngestClient(
        client_name=f"TX_SIM_{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        pipe_name=PIPE,
        profile_json="profile.json",
    ) as client:

        
        channel, status = client.open_channel(channel_name)
        last_offset = status.latest_committed_offset_token
        seq = int(last_offset) + 1 if last_offset is not None else 0

        print(f"Channel '{channel_name}' opened. Resuming from sequence {seq}.\n")

        while True:

            tx = make_transaction(seq)

            channel.append_row(tx, str(seq))
            channel.initiate_flush()
            channel.wait_for_flush()

            print(
                f"[{tx['ts'].strftime('%H:%M:%S')}] "
                f"seq={seq} "
                f"{tx['merchant']} "
                f"{tx['currency']} "
                f"{tx['amount']} "
                f"{tx['status']}"
            )

            seq += 1

            time.sleep(INTERVAL_SECONDS)

def make_transaction(seq):
    return {
        "transaction_id": str(uuid.uuid4()),
        "sequence": seq,
        "merchant": random.choice(MERCHANTS),
        "amount": round(random.uniform(10, 500), 2),
        "currency": random.choice(CURRENCIES),
        "status": random.choice(STATUSES),
        "ts": datetime.utcnow()
    }

if __name__ == "__main__":
    main()