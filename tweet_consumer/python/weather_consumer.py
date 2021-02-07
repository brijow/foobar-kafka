from tweet_analytics import *
from kafka import KafkaConsumer
import nltk, os, pickle
from sklearn.pipeline import Pipeline
import ast



if __name__ == "__main__":
    print("Starting Weather Consumer")
    TOPIC_NAME = os.environ.get("TOPIC_NAME")
    KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
    
    print("Setting up Kafka consumer at {}".format(KAFKA_BROKER_URL))
    consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[KAFKA_BROKER_URL + ':9092'])
    
    print('Waiting for msg...')
    for msg in consumer:
        # print('got one!')
        msg = msg.value.decode('ascii')
        if isinstance(msg, list): 
            target = msg
        else :
            target = ast.literal_eval(msg)
        print("Got {} target weather reports".format(len(target)))
        print(target)
    print("Bye-Bye")