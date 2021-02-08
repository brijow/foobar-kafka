import json
import os
from datetime import datetime, timezone 
import tweepy
from kafka import KafkaProducer

METRO_VANCOUVER_GEOBOX = [-123.371556,49.009125,-122.264683,49.375294]

# Twitter API Keys
access_token = "1354933647602720781-kyLKKAzWlA46h26MTT4Fjr2fMf3Die"
access_token_secret = "UePNCQbTz7lqjPpmaqUXd63eKGuR1cMhm4pbBFURsqRmW"
consumer_key = "BbS4k0PqSa3y80IFnhUSjOXz4"
consumer_secret = "GSE59GfdAsBwrN3S7c7s0GEuQPDTb4ilMNu0ry3iqwV0OYWVzm"

# Kafka settings
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL") if os.environ.get("KAFKA_BROKER_URL") else 'localhost:9092'
TOPIC_NAME = os.environ.get("TOPIC_NAME") if os.environ.get("TOPIC_NAME")  else 'from_twitter'


class stream_listener(tweepy.StreamListener):

    def __init__(self):
        super(stream_listener, self).__init__()
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda x: json.dumps(x).encode('utf8'),
            api_version=(0, 10, 1)
        )

    def on_status(self, status):
        tweet = status.text
        twitter_df = {
            'tweet':tweet,
            'datetime': datetime.utcnow().timestamp(),
            'location': 'MetroVancouver'
        }
        print(tweet)
        
        self.producer.send(TOPIC_NAME, value = twitter_df)
        self.producer.flush()
        # print('a tweet sent to kafka')
        return True

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False
        print('Streaming Error: '+str(status_code))

class twitter_stream():

    def __init__(self):
        self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        self.auth.set_access_token(access_token, access_token_secret)
        self.stream_listener = stream_listener()
    
    def twitter_listener(self):
        stream = tweepy.Stream(auth = self.auth, listener=self.stream_listener)
        stream.filter(locations=METRO_VANCOUVER_GEOBOX)

if __name__ == '__main__':
    ts = twitter_stream()
    ts.twitter_listener()


