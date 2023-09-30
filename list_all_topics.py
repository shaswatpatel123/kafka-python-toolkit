from kafka import KafkaConsumer
import logging

logging.basicConfig(format='[%(asctime)s] [%(module)s] [%(levelname)s] %(message)s',level=logging.INFO)

def listAllTopics(brokerlist):
    logging.info('Creating Kafka consumer from brokers')
    
    try:
        consumer = KafkaConsumer(bootstrap_servers=brokerlist)
    except Exception as e:
        logging.error('Error creating Kafka consumer' + str(e))
        return []

    topics = consumer.topics()
    consumer.close()
    logging.info('Kafka consumer closed')
    return topics


if __name__ == "__main__":
    logging.info('Reading brokers from brokerlis.txt')
    brokers = (open('brokerlist.txt','r').read()).replace('\n','').split(',')
    allTopics = listAllTopics(brokers)

    for topic in allTopics:
        print(topic)
