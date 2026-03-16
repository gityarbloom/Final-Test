from confluent_kafka import Producer, Consumer
import json



class KafkaProdConsum:

    def __init__(self, prod_config =None, consum_config =None):
        if prod_config:
            self.producer = Producer(prod_config)
        if consum_config:
            self.consumer = Consumer(consum_config)
        else:
            raise Exception("No instance of the model was created because no configurations were received.")
    
    
    def publish_to_kafka(self, topic_name: str, event, total:int):
        try:
            event = json.dumps(event).encode('utf-8')
            self.producer.produce(topic=topic_name, value=event, callback=self.delivery_report)
            print(f"published Event number {total} to Kafka Topic named {topic_name}")
            self.producer.flush()
        except Exception:
            raise Exception(f"Kafka Producer Failed")


    def consum_from_kafka(self, topic_name: str):
        self.consumer.subscribe([topic_name])
        print(f"Kafka-onsumer is running and subscribed to {topic_name} topic")
        try:
            counter = 0
            while True:
                counter += 1
                msg =self.consumer.poll(1.0)
                if msg is None:
                    print(f"\nconsumer retry: {counter}\n")
                    continue
                if msg.error():
                    print("Kafks Error:", msg.error())
                    continue
                value = msg.value().decode('utf-8')
                doc = json.loads(value)
                yield doc
        except Exception as e:
            raise Exception(f"Kafka consuming failed. \n{e}")
        
        
    @staticmethod
    def delivery_report(err, msg):
        if err:
            print(f"❌ Delivery failed: {err}")
        else:
            print(f"✅ Delivered {msg.value().decode('utf-8')} to {msg.topic()} : \npartition {msg.partition()} : at offset {msg.offset()}")