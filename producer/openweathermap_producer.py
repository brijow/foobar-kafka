"""Produce dumb openweathermap content to 'openweathermap.topic' kafka topic."""
import asyncio
import json
import os
from collections import namedtuple
from dataprep.connector import connect
from kafka import KafkaProducer
from time import sleep

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TOPIC_NAME = os.environ.get("TOPIC_NAME")
SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 600))

ApiInfo = namedtuple('ApiInfo', ['name', 'access_token'])
apiInfo = ApiInfo('openweathermap', '73d0140e5ab3cfac25c117068562e17e')

sc = connect(apiInfo.name,
             _auth={'access_token': apiInfo.access_token},
             _concurrency=3)


async def get_weather(city):
    """Get the current weather details for the given city.

    Note - Description returns null for now. 
    """
    df_weather = await sc.query("weather", q=city)
    return df_weather


def run():
    kafkaurl = KAFKA_BROKER_URL
    print("Setting up Weather producer at {}".format(kafkaurl))
    producer = KafkaProducer(
        bootstrap_servers=[kafkaurl],
        # Encode all values as JSON
        value_serializer=lambda x: json.dumps(x).encode('ascii'),
    )

    while True:
        current_weather = asyncio.run(get_weather(city="Vancouver"))
        producer.send(TOPIC_NAME, value=current_weather)
        sleep(SLEEP_TIME)


if __name__ == "__main__":
    run()
