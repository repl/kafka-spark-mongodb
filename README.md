# kafka-spark-mongodb

### Objective
Load data from files to mongodb using kafka and spark
* Load data from CSV to Kafka (Kafka producer)
* Read messages from Kafka (Kafka consumer) as spark RDD and it save into mongoDb

### Running Kafka locally
cd /mnt/work/installedApps/kafka_2.11-2.1.0
#### Start ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
#### Start Kafka
bin/kafka-server-start.sh config/server.properties
#### Create topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic TopicBooks
#### Monitor topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic TopicBooks --from-beginning

### Running Local Kafka Cluster
A containerized kafka cluster, with a broker listening on localhost:9092. A docker-compose config file is provided, to run a single broker kafka cluster + single node zookeeper.

To start it:
$ docker-compose -f kafka-docker/docker-compose.yml up -d

And to shut it down:
docker-compose -f kafka-docker/docker-compose.yml down
The ports are 2181 for ZooKeeper and 9092 for Kafka

#### Run programs with sbt

``` shell
sbt test:compile
sbt runMain org.repl.kafkasparkmongo.BXBooksLoader
```
