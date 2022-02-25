# foobar-kafka 🌞
This project aims to implement a simple, but general architecture for supporting fault-tolerant data pipeline applications by utilising Kafka, Docker, and Cassandra as a collection of microservices. To demonstrate the potential feasibility of this architecture, we implement an example data-driven application to explore statistical relationships between Twitter sentiment and the weather.


## Quickstart instructions

> **Note:** The following instructions are for bringing up the entire system locally on a single machine. This requires about 6G of RAM available to your Docker client. See the [About section](#about) at the end of this readme for additional information regarding background context and link to a complimentary blog post, which provides a more comprehensive and simplified overview of the project.
>
> Docker networking diagram / container connectivity:
> <p align="center"><img src="https://user-images.githubusercontent.com/11220949/155803461-924ae67f-ad0b-41e6-8246-c442e7a5ad34.png" width="60%"> </p>

### Instantiate services for defining the architecture

```bash
$ docker network create kafka-network                         # create network for kafka cluster (zookeeper, broker, kafka-manager services, and kafka connect sink services)

$ docker network create cassandra-network                     # create network for cassandra. (kafka connect will exist on this network as well in addition to kafka-network)

$ docker-compose -f cassandra/docker-compose.yml up -d        # start Cassandra, (runs keyspace and schema creation scripts at during setup)

$ docker-compose -f kafka/docker-compose.yml up -d            # start single zookeeper, broker, kafka-manager and kafka-connect services

$ docker ps -a                                                # sanity check to make sure services are up: kafka_broker_1, kafka-manager, zookeeper, kafka-connect service
```

> **Note:** 
Kafka front end at this point should be available at http://localhost:9000
>
> Kafka-Connect REST interface is available at http://localhost:8083

### Start producers and consumers
```bash
$ docker-compose -f owm-producer/docker-compose.yml up -d     # start the producer that retrieves open weather map

$ docker-compose -f twitter-producer/docker-compose.yml up -d # start the producer for twitter

$ docker-compose -f consumers/docker-compose.yml build

$ docker-compose -f consumers/docker-compose.yml up -d        # start the consumers

$ docker ps -a                                                # sanity check: make sure services are up: kafka_broker_1, kafka-manager, zookeeper, kafka-connect service
```


### Check data is being sent to Cassandra

First login into Cassandra's container with the following command or open a new CLI from Docker Desktop if you use that.
```bash
$ docker exec -it cassandra bash
```

Once loged in, bring up cqlsh with this command and query twitterdata and weatherreport tables like this:
```bash
$ cqlsh --cqlversion=3.4.4 127.0.0.1 #make sure you use the correct cqlversion

cqlsh> use kafkapipeline; #keyspace name

cqlsh:kafkapipeline> select * from twitterdata;

cqlsh:kafkapipeline> select * from weatherreport;
```

And that's it! you should be seeing records coming in to Cassandra. Feel free to play around with it by bringing down containers and then up again to see the magic of fault tolerance!

### Load data utility
To load backup CSV files into Cassandra, from a console run the following:

```bash
$ python consumers/python/cassandrautils.py twitter {PATH_TO_twitter.csv}
$ python consumers/python/cassandrautils.py weather {PATH_TO_weather.csv}
```

### Teardown
To stop all running kakfa cluster services:

```bash
$ docker-compose -f consumers/docker-compose.yml down          # stop the consumers

$ docker-compose -f owm-producer/docker-compose.yml down       # stop open weather map producer

$ docker-compose -f twitter-producer/docker-compose.yml down   # stop twitter producer

$ docker-compose -f kafka/docker-compose.yml down              # stop zookeeper, broker, kafka-manager and kafka-connect services

$ docker-compose -f cassandra/docker-compose.yml down          # stop Cassandra
```

To remove the kafka-network network:

```bash
$ docker network rm kafka-network

$ docker network rm cassandra-network
```

## About
This repo was created as a mid-term project milestone for SFU's Big Data Lab 2 (CMPT 733), authored by Brian White, Sebastian Alcaino, Vignesh Perumal, and Babak Badkoubeh. Since we didn't have much idea for a group name, "foobar" ended up sticking. This codebase manifested into a term-end project which can be found in the [brijow/foobar-gamestop](https://github.com/brijow/foobar-gamestop) repo. Though these projects have diverged over the term (they demonstrate two seemingly very different applications), the Kafka-focused microservice architecture established in this project is the key foundational component in each.

A complimentary blog post for this project can be found on SFU's medium page, [here](https://medium.com/sfu-cspmp/building-data-pipeline-kafka-docker-4d2a6cfc92ca).

