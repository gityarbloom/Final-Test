from kafka_model import KafkaProdConsum
from validation import EventModel, ValidationError
from sql_model import MySqlConnection
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
sql = MySqlConnection()


kafka = KafkaProdConsum(p_config, c_config)

for event in kafka.consum_from_kafka("intel"):
    try:
        valid_event = EventModel(**event)
    except ValidationError as e:

