Apache Kafka install and usage: (--version 4.1.1-3) whithout zookeeper , with controller (KRaft)

1) sudo dnf install apache-kafka 


2) sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
    -t $(/opt/kafka/bin/kafka-storage.sh random-uuid) \
    -c /opt/kafka/config/server.properties \
    --standalone

3) config been in /opt/kafka/config/server.properties and template in ./server.properties.template

4)  create the topic : bin/kafka-topics.sh --create --topic <topic_name> --bootstrap-server <broker_address> --partitions <count> --replication-factor <count>
