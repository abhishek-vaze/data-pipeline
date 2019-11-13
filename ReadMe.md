Start ZooKeeper
open command prompt type command zkserver

Start Kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties

Kafka Create Topic for Input 

kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic error_input

Kafka Create Topic for Output
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic error_output

Run Kafka Consumer

kafka-console-consumer.bat --bootstrap-server localhost:9092  --topic error_input