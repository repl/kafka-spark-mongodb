# kafka-spark-mongodb
Load data from files to mongodb using kafka and spark

Load data from CSV to Kafka (Kafka producer)
Read messages from Kafka (Kafka consumer) as spark RDD and it save into mongoDb


### Running Local Kafka Cluster
To run the demo, you will need a local kafka cluster, with a broker listening on localhost:9092. A docker-compose config file is provided, to run a single broker kafka cluster + single node zookeeper.

To start it:

$ docker-compose -f kafka-docker/docker-compose.yml up -d
And to shut it down:

docker-compose -f kafka-docker/docker-compose.yml down
The ports are 2181 for ZooKeeper and 9092 for Kafka