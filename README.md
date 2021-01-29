# Quickstart instructions

## Starting kafka on docker

```bash
$ docker network create kafka-network                  # create a new docker network for kafka cluster (zookeeper, broker, kafka-manager services)
$ docker-compose -f kafka/docker-compose.yml up -d     # start single zookeeper, broker, and kafka-manager services
$ docker-compose -f producer/docker-compose.yml up -d  # start the producer that downloads spam email data and then sends random samples to kafka topic
$ docker ps -a                                         # sanity check to make sure services are up: kafka_broker_1, kafka-manager, and zookeeper
```

> **Note:** Kafka front end is available at http://localhost:9000


## Teardown

To stop all running kakfa cluster services

```bash
$ docker-compose -f kafka/docker-compose.yml down      # stop zookeeper, broker, and kafka-manager services
$ docker-compose -f producer/docker-compose.yml down   # stop the producer
```

To remove the kafka-network network:

```bash
$ docker network rm kafka-network
```

## FAQs

How can I connect to a running container?

```bash
docker exec -it <container_name>
```
