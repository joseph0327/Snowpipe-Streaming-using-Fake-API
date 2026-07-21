import requests
import time
import uuid
from snowflake.ingest.streaming import StreamingIngestClient

API_URL="http://127.0.0.1:8000/api/v1/patient-events"

DATABASE="HEALTH_DB"
SCHEMA="HEALTH_SCHEMA"
TABLE="PATIENT_EVENTS_STREAM"
PIPE="PATIENT_EVENTS_STREAM-STREAMING"

BATCH_SIZE=10
FLUSH_INTERVAL=5

client_name=f"HEALTH_STREAM_{uuid.uuid4()}"

with StreamingIngestClient(
    client_name=client_name,
    db_name=DATABASE,
    schema_name=SCHEMA,
    pipe_name=PIPE,
    profile_json="profile.json"
) as client:

    channel,status=client.open_channel("HEALTH_CHANNEL")

    last_offset=status.latest_committed_offset_token

    sequence=int(last_offset)+1 if last_offset else 0

    buffer=[]

    while True:

        try:

            response=requests.get(
                API_URL,
                timeout=10
            )

            events=response.json()

            print("\n========== NEW STREAM BATCH ==========")
            print("Received",len(events),"health events")


            for event in events:

                print("--------------------------------------")
                print("Event ID:",event["event_id"])
                print("Patient ID:",event["patient_id"])
                print("Device ID:",event["device_id"])
                print("Event Type:",event["event_type"])
                print("Heart Rate:",event["heart_rate"],"bpm")
                print(
                    "Blood Pressure:",
                    event["blood_pressure"]["systolic"],
                    "/",
                    event["blood_pressure"]["diastolic"]
                )
                print("Temperature:",event["temperature"],"°C")
                print("Oxygen Level:",event["oxygen_level"],"%")
                print("Status:",event["status"])
                print("Event Time:",event["event_time"])


                channel.append_row(
                    event,
                    str(sequence)
                )


                buffer.append(event)

                sequence+=1


                if len(buffer)>=BATCH_SIZE:

                    print(
                        "Flushing",
                        len(buffer),
                        "events to Snowflake"
                    )

                    channel.initiate_flush()

                    channel.wait_for_flush()

                    buffer.clear()


            print("======================================\n")

            time.sleep(FLUSH_INTERVAL)


        except Exception as e:

            print("Streaming error:",e)

            time.sleep(5)