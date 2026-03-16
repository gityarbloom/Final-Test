from confluent_kafka import Producer, Consumer
import json



class KafkaProdConsum:

    def __init__(self, prod_config =None, consum_config =None):
        try:
            if not prod_config and not consum_config:
                raise Exception("No instance of the model was created because no configurations were received.")
            if prod_config:
                self.producer = Producer(prod_config)
                print("Kafka-Producer configured")
            if consum_config:
                self.consumer = Consumer(consum_config)
                print("Kafka-Consumer configured")
        except Exception:
            raise

    
    
    def publish_to_kafka(self, topic_name: str, event, total:int):
        try:
            event = json.dumps(event).encode('utf-8')
            self.producer.produce(topic=topic_name, value=event, callback=self.delivery_report)
            self.producer.flush()
        except Exception:
            raise

    def consum_from_kafka(self, topic_name: str):
        self.consumer.subscribe([topic_name])
        print(f"Kafka-onsumer is running and subscribed to {topic_name} topic")
        try:
            counter = 0
            while True:
                counter += 1
                msg =self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Kafks Error:", msg.error())
                    continue
                value = msg.value().decode('utf-8')
                doc = json.loads(value)
                yield doc
        except Exception:
            raise

        
    @staticmethod
    def delivery_report(err, msg):
        if err:
            print(f"Delivery failed. \nDetails: \n{err}")
        else:
            print(f"Delivered {msg.value().decode('utf-8')} \nto {msg.topic()} : \npartition {msg.partition()}")