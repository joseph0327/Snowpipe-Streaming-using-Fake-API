from fastapi import FastAPI
import random
import uuid
from datetime import datetime

app = FastAPI(
    title="Health Data Streaming API",
    version="1.0"
)

PATIENTS = ["PAT1001","PAT1002","PAT1003","PAT1004"  ]
DEVICES = ["SMARTWATCH","ECG_MONITOR","BP_MONITOR"]

EVENT_TYPES = ["HEART_RATE","BLOOD_PRESSURE","TEMPERATURE","OXYGEN_LEVEL"]

STATUSES = ["NORMAL","WARNING","CRITICAL"]


@app.get("/")
def home():
    return {
        "message": "Health API running"
    }

@app.get("/api/v1/patient-events")
def get_patient_events():

    events = []

    for i in range(5):

        event = {
            "event_id": str(uuid.uuid4()),
            "patient_id": random.choice(PATIENTS),
            "device_id": random.choice(DEVICES),
            "event_type": random.choice(EVENT_TYPES),
            "heart_rate": random.randint(50,120),
            "blood_pressure": {
                "systolic": random.randint(110,150),
                "diastolic": random.randint(70,95)
            },
            "temperature": round(random.uniform(36,39),2),
            "oxygen_level": random.randint(90,100),
            "status": random.choice(STATUSES),
            "event_time": datetime.utcnow().isoformat()
        }

        events.append(event)

    return events