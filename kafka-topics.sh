# Start Zookeeper
./bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server
./bin/kafka-server-start.sh config/server.properties 

# List topics on the server
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create topic for data
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic !!TOPIC_NAME!!

# Start console consumer
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic !!TOPIC_NAME!!

# Start console producer
./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic !!TOPIC_NAME!!
