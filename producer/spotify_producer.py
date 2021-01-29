"""Produce dumb spotify content to 'spotify.topic' kafka topic."""
import asyncio
import json
import os
from collections import namedtuple
from dataprep.connector import connect
from kafka import KafkaProducer
from time import sleep

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TOPIC_NAME = os.environ.get("TOPIC_NAME")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE"))
SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 10))

ApiInfo = namedtuple('ApiInfo', ['name', 'client_id', 'client_secret'])
apiInfo = ApiInfo('spotify', '0448536050d84126a6b0603823365405',
                  'e9704e2008bf4378aabdd2616511b6bc')

sc = connect(apiInfo.name,
             _auth={'client_id': apiInfo.client_id,
                    'client_secret': apiInfo.client_secret},
             _concurrency=3)


async def get_message_batch(n=BATCH_SIZE):
    """Get artists names returned from this query as a list.

    Note - we query for Beyonce, but there are many other artists
    that have Beyonce as part of their name! For example, "Lil Beyonce".
    We query for BATCH_SIZE results, although the maximum returned
    will likely be around 22, or so.
    """
    df_beyonce = await sc.query("artist", q="Beyonce", _count=BATCH_SIZE)
    batch = df_beyonce['name'].tolist()
    return batch


def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda x: json.dumps(x).encode('ascii'),
    )

    while True:
        msg_batch = asyncio.run(get_message_batch())
        producer.send(TOPIC_NAME, value=msg_batch)
        sleep(SLEEP_TIME)


if __name__ == "__main__":
    run()
