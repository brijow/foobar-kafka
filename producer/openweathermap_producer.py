"""Produce openweathermap content to 'weather' kafka topic."""
import asyncio
import json
import os
import time
from collections import namedtuple
from dataprep.connector import connect
from kafka import KafkaProducer


KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TOPIC_NAME = os.environ.get("TOPIC_NAME")
SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 60))

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
    locations = ["Vancouver"]
    iterator = 0
    repeat_request = SLEEP_TIME/len(locations)
    print("Setting up Weather producer at {}".format(kafkaurl))
    producer = KafkaProducer(
        bootstrap_servers=kafkaurl,
        # Encode all values as JSON
        value_serializer=lambda x: str(x).encode('ascii'),
    )

    while True:
        location = locations[(iterator+1) % len(locations)]
        current_weather = asyncio.run(get_weather(city=location))
        current_weather['location'] = location
        now = time.localtime()
        current_weather['report_time'] = time.strftime(
            "%Y-%m-%d %H:%M:%S", now)
        current_weather = current_weather.to_json()
        # adding prints for debugging in logs
        print("Sending new weather report iteration - {}".format(iterator))
        producer.send(TOPIC_NAME, value=current_weather)
        print("New weather report sent")
        time.sleep(repeat_request)
        print("Waking up!")
        iterator += 1


if __name__ == "__main__":
    run()
