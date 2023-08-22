export KAFKA_HOME=/home/ahmed/Documents/kafka-3.4.0-src

# Start Zookeeper
${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_HOME}/config/zookeeper.properties

# Start Kafka server
${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties 

# List topics on the server
${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create topic for data
${KAFKA_HOME}/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic line_data

# Start console consumer
${KAFKA_HOME}//bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic line_data

# Start console producer
${KAFKA_HOME}//bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic line_data
