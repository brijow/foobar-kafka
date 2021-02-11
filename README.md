# Quickstart instructions

## Starting kafka on docker

```bash
$ docker network create kafka-network                  # create a new docker network for kafka cluster (zookeeper, broker, kafka-manager services)
$ docker-compose -f kafka/docker-compose.yml up -d     # start single zookeeper, broker, and kafka-manager services
$ docker-compose -f producer/docker-compose.yml up -d  # start the producer that downloads spam email data and then sends random samples to kafka topic
$ docker ps -a                                         # sanity check to make sure services are up: kafka_broker_1, kafka-manager, and zookeeper
```

> **Note:** Kafka front end is available at http://localhost:9000

## Starting Cassandra

Cassandra is setup so it runs keyspace and schema creation scripts at first setup so it is ready to use.
$ docker-compose -f cassandra/docker-compose.yml up -d

## Starting Twitter classifier plus Weather consumer

(Alternatively you can build first, but is not necessary, docker will do it anyway if it hasnt build before, but it is required to apply new changes)
$ docker-compose -f twitter_service/docker-compose.yml build

Start twitter_service:
$ docker-compose -f twitter_service/docker-compose.yml up -d # start the producer that downloads spam email data and then sends random samples

## Teardown

To stop all running kakfa cluster services

```bash
$ docker-compose -f tweet_consumer/docker-compose.yml down
$ docker-compose -f kafka/docker-compose.yml down      # stop zookeeper, broker, and kafka-manager services
$ docker-compose -f producer/docker-compose.yml down   # stop the producer
$ docker-compose -f cassandra/docker-compose.yml down # stop Cassandra
```

To remove the kafka-network network:

```bash
$ docker network rm kafka-network
```

## Load data utility

From a console run the following:

```bash
$ python consumers/python/cassandrautils.py twitter {PATH_TO_twitter.csv}
$ python consumers/python/cassandrautils.py weather {PATH_TO_weather.csv}
```

## FAQs

How can I connect to a running container?

```bash
docker exec -it <container_name>
```
