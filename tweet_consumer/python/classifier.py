from tweet_analytics import *
from kafka import KafkaConsumer
import nltk, os, pickle
from sklearn.pipeline import Pipeline
import ast



if __name__ == "__main__":

    path = os.path.dirname(os.path.realpath(__file__))
    parent = os.path.dirname(path)
    cwd = parent + "/nltk_data"
    print("Set NLTK path to {}".format(cwd))
    nltk.data.path = [cwd]


    TOPIC_NAME = os.environ.get("TOPIC_NAME")
    KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
    print("Reading model file")
    with open(path + '/trainedpipe.pkl', 'rb') as f:
        classifier = pickle.load(f)
    print("Setting up Kafka consumer at {}".format(KAFKA_BROKER_URL))
    consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[KAFKA_BROKER_URL + ':9092'])
    
    print('Waiting for msg...')
    for msg in consumer:
        # print('got one!')
        msg = msg.value.decode('utf-8')
        if isinstance(msg, list): 
            target = msg
        else :
            target = ast.literal_eval(msg)
        print("Got {} target tweets".format(len(target)))
        res = classifier.predict(target)
        if len(target) > 1:
            for i, pred in enumerate(res):
                t = target[i]
                print("{} :{}".format(t, "Positive" if pred == 1 else "Negative"))
        else:
            print(msg, "Positive" if res == 1 else "Negative")
    print("Bye-Bye")