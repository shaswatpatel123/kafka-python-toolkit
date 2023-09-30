from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(
    format='[%(asctime)s] [%(module)s] [%(levelname)s] %(message)s', level=logging.INFO)


def consumerFromTopic(brokerList):
    logging.info('Creating Kafka consumer from brokers')

    try:
        consumer = KafkaConsumer(bootstrap_servers=brokerList, auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        
        # Provide topic name here
        consumer.subscribe(['test-topic'])
    except Exception as e:
        logging.error('Error creating Kafka consumer' + str(e))
        return

    for message in consumer:
        message = message.value
        logging.info(message)


if __name__ == "__main__":
    logging.info('Reading brokers from brokerlis.txt')
    brokers = (open('brokerlist.txt', 'r').read()).replace('\n', '').split(',')
    consumerFromTopic(brokers)
