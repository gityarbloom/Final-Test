from kafka_model import KafkaProdConsum
from validation import EventModel, ValidationError
from sql_model import MySqlTargetDB
import logger
import os




kafka_boots_etc = os.getenv("kafka_boots_etc")

p_config = {"bootstrap.servers": kafka_boots_etc}
c_config = {
    'bootstrap.servers': kafka_boots_etc,
    'group.id': 'mygroup',
    'session.timeout.ms': 6000,
    'auto.offset.reset': 'earliest'
        }
sql = MySqlTargetDB()


kafka = KafkaProdConsum(p_config, c_config)

for event in kafka.consum_from_kafka("intel"):
    try:
        signal_id = event["signal_id"]
        entity_id = event["entity_id"]
        if not sql.is_destroyed(entity_id):
            try:
                valid_event = EventModel(**event)           
            except ValidationError as e:
                print(f"notification {signal_id} is not valid")
        else:
            kafka.publish_to_kafka("dlq_signals_intel", event=event)
        if not sql.is_exists_in_target_banck(entity_id=entity_id):
            event["priority_level"] = 99
            event["speed"] = 0
            
            sql.insert_a_new_target()

    except Exception as e:
        raise