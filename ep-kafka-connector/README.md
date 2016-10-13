# Kafka Connector with Spark Streaming

This POC shows the capability of Offset management in Zookeeper in ZNodes. This includes a sample Wordcount program in Spark Streaming which is managing the Offset as well in the Zookeeper Znode.

## Download Kafka 0.9.0.1
And install it on your windows system. And from the "windows" directory in installation execute below commands.

## Start Services using :

zookeeper-server-start.bat ../../config/zookeeper.properties
kafka-server-start.bat ../../config/server.properties

## Create topics and sample producer :

kafka-topics.bat --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic test
kafka-topics.bat --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic test2
kafka-topics.bat --list --zookeeper 127.0.0.1:2181

kafka-console-producer.bat --broker-list 127.0.0.1:9092 --topic test
kafka-console-producer.bat --broker-list 127.0.0.1:9092 --topic test2

zookeeper-shell 127.0.0.1:2181

## To run the Program 

### In Eclipse/Java, pass the following in arguments :

	127.0.0.1:9092 test,test2 Spark
	