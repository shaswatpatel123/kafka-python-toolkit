from fastapi import FastAPI, Header
from typing import Union
from kafka import KafkaConsumer, TopicPartition, KafkaAdminClient
from kafka.errors import KafkaError

app = FastAPI()

@app.post("/check-lag")
def checkLag(consumer_group: Union[str, None] = Header(default=None),topic: Union[str, None] = Header(default=None), brokerlist: Union[str, None] = Header(default=None)):
    toReturn = dict()
    try:
        brokerlist = (brokerlist.replace("\n","")).split(",")
        topic_name = topic

        # Connect to Kafka
        consumer = KafkaConsumer(bootstrap_servers=brokerlist,
                                group_id=consumer_group,
                                enable_auto_commit=False)
        admin_client = KafkaAdminClient(bootstrap_servers=brokerlist)

        try:
            # Get the partitions for the topic
            partitions = consumer.partitions_for_topic(topic_name)
            topic_partitions = [TopicPartition(topic_name, p) for p in partitions]

            # Get the latest offsets for the topic
            latest_offsets = consumer.end_offsets(topic_partitions)

            # Get the committed offsets for the consumer group
            committed_offsets = {}
            for tp in topic_partitions:
                committed = consumer.committed(tp)
                committed_offsets[tp] = committed if committed else 0

            # Calculate the lag for each partition
            lags = {}
            for tp in topic_partitions:
                lags[tp.partition] = latest_offsets[tp] - committed_offsets[tp]

            # Print the lag
            print("Lag for consumer group '{}' and topic '{}':".format(consumer_group, topic_name))
            for partition, lag in lags.items():
                toReturn[partition] = lag

        except KafkaError as ke:
            print("Error while fetching lag:", ke)
        finally:
            consumer.close()
            admin_client.close()
            return toReturn
    except Exception as e:
        return {
            "exception" : str(e)
        }

@app.get("/")
async def root():
    return {"message": "Hello World"}
