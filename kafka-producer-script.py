from kafka import KafkaProducer
import json
import logging
from kafka.errors import KafkaError
import uuid
import datetime

logging.basicConfig(format='[%(asctime)s] [%(module)s] [%(levelname)s] %(message)s',level=logging.INFO)

def produceMessages(brokerList):
    producer = KafkaProducer(bootstrap_servers=brokerList,
                            value_serializer=lambda x: 
                            json.dumps(x).encode('utf-8'))
    
    for _ in range(10):
        payload = {
            "uuid" : str(uuid.uuid4()),
            "timestamp" : str(int(datetime.datetime.timestamp(datetime.datetime.now())*1000))
        }
        try:
            res = producer.send('test-topic', value=payload)
            recordMetadata = res.get(timeout=10)
            logging.info(recordMetadata.topic)
            logging.info(recordMetadata.timestamp)
            logging.info(recordMetadata.partition)
            logging.info(recordMetadata.offset)
            logging.info(type(recordMetadata))
            logging.info(recordMetadata)
        except KafkaError as e:
            logging.error(str(e))


if __name__ == "__main__":
    logging.info('Reading brokers from brokerlis.txt')
    brokers = (open('brokerlist.txt','r').read()).replace('\n','').split(',')
    produceMessages(brokers)