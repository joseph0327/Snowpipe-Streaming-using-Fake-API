# Snowflake Streaming Ingest Pipelines

![Snowflake](https://img.shields.io/badge/Snowflake-Streaming%20Ingest-29B5E8?style=for-the-badge&logo=snowflake)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

## Real-Time Data Engineering with Snowflake Streaming Ingest API

This project demonstrates real-time event streaming into Snowflake using the **Snowflake Streaming Ingest API**.

Two enterprise-style streaming pipelines are implemented:

1. **Bank Transaction Streaming Pipeline**
2. **Healthcare Patient Event Streaming Pipeline**

Both pipelines use Python applications to generate or consume events, buffer records, and stream data continuously into Snowflake.

---

# Architecture

## Banking Transaction Stream
MOBILE APP
|
WEB APP
|
ATM SYSTEM
|
v
Python Streaming Producer
|
v
Snowflake Streaming Ingest API
|
v
TRANSACTIONS_TABLE_STREAMING


## Healthcare Event Stream
Patient Monitoring API
|
v
Python Streaming Consumer
|
v
Snowflake Streaming Ingest API
|
v
PATIENT_EVENTS_STREAM
---

# Technologies

| Technology | Usage |
|---|---|
| Python | Streaming applications |
| Snowflake Streaming Ingest API | Real-time ingestion |
| Snowflake | Cloud data warehouse |
| Snowflake Key Pair Authentication | Secure access |
| REST API | Healthcare event source |
| SQL | Infrastructure deployment |

---


---

# Pipeline 1: Banking Transaction Streaming

## Overview

Simulates a banking platform receiving transactions from multiple producers:

- MOBILE_APP
- WEB_APP
- ATM_SYSTEM


Each event contains:

| Field | Description |
|---|---|
| transaction_id | Unique transaction ID |
| sequence | Streaming sequence number |
| source | Producer source |
| customer_id | Customer identifier |
| account_id | Account identifier |
| merchant | Merchant name |
| transaction_type | Purchase, payment, transfer, withdrawal |
| amount | Transaction value |
| currency | GBP, USD, EUR |
| status | Approved, declined, pending |
| channel | Mobile, Web, ATM |
| country | Transaction location |
| device_id | Device identifier |
| reference_number | Transaction reference |
| event_time | Event timestamp |

---

# Pipeline 2: Healthcare Patient Event Streaming

## Overview

Consumes patient monitoring events from a REST API and streams them into Snowflake.

Captured metrics:

- Patient ID
- Device ID
- Event type
- Heart rate
- Blood pressure
- Temperature
- Oxygen level
- Patient status
- Event timestamp


---

# Snowflake Infrastructure

The SQL scripts create the required Snowflake environment.

## Banking Objects
Database:
TRANSACTIONSBD

Schema:
TRANSACTION_SCHEMA

Warehouse:
TRANSACTION_WH

Role:
TRANSACTION_USER_ROLE

Table:
TRANSACTIONS_TABLE_STREAMING


---

## Healthcare Objects
Database:
HEALTH_DB

Schema:
HEALTH_SCHEMA

Warehouse:
HEALTH_WH

Role:
HEALTH_ROLE

Table:
PATIENT_EVENTS_STREAM


---

# Authentication Setup

This project uses Snowflake Key Pair Authentication.

Generate RSA keys:

- bash:
openssl genrsa 2048 | openssl pkcs8 -topk8 \
-inform PEM -out rsa_key.p8 -nocrypt

openssl rsa -in rsa_key.p8 \
-pubout -out rsa_key.pub


- Add the public key:
ALTER USER USERNAME
SET RSA_PUBLIC_KEY='PUBLIC_KEY';

- Configuration
Create: config/profile.json

Example:
{
    "account": "ACCOUNT_IDENTIFIER",
    "user": "USERNAME",
    "private_key_file": "rsa_key.p8",
    "role": "TRANSACTION_USER_ROLE"
}

---
# Requirements
snowpipe-streaming
snowflake-ingest
snowflake-connector-python
requests
fastapi
uvicorn
---
# Run Transaction Streaming
 python transaction_stream.py

Example result:
MOBILE_APP created transaction 20 PURCHASE 250 GBP
WEB_APP created transaction 21 PAYMENT 90 USD
Flushing MOBILE_APP 10 rows

Run Healthcare Streaming
Start API: patient_api.py
Start consumer: health_stream.py

Example result:
Received 10 health events
Patient ID: PAT1001
Heart Rate: 78 bpm
Temperature: 36.7 C
Oxygen Level: 98 %
Flushing 10 events to Snowflake

---

# Validate Data in Snowflake
- Transactions:
SELECT * FROM TRANSACTIONSBD.TRANSACTION_SCHEMA.TRANSACTIONS_TABLE_STREAMING;

- Healthcare:
SELECT * FROM HEALTH_DB.HEALTH_SCHEMA.PATIENT_EVENTS_STREAM;

---
# Skills Demonstrated
✔ Snowflake Streaming Ingest API
✔ Real-time data pipelines
✔ Python event processing
✔ Batch ingestion strategies
✔ Snowflake RBAC
✔ Key Pair authentication
✔ REST API ingestion
✔ Cloud data engineering architecture
