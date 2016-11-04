# Kafka Connector with Spark Streaming

This POC shows the capability of Offset management of multiple topics in Zookeeper in ZNodes for Direct Kafka Stream (Scala & Java both included). This includes a sample Wordcount program in Spark Streaming which is managing the Offset as well in the Zookeeper Znode.

## Download & Install Kafka 0.9.0.1
And install it on your windows system. And from the "windows" directory in installation execute below commands.

<For Distributed upcoming>

## Start Services using :

Change Directory to :

	cd C:\Softwares\kafka\bin\windows

To start the internal zookeeper, you can use the below command.

	zookeeper-server-start.bat ../../config/zookeeper.properties
	
To Start the Kafka server with one broker, you can use the below command.

	kafka-server-start.bat ../../config/server.properties

## Create topics and sample producer :

To create topics in Kafka, Use the below.

	kafka-topics.bat --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic test
	kafka-topics.bat --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic test2
	
To List the multiple topics use.
 
	kafka-topics.bat --list --zookeeper 127.0.0.1:2181
	
To open Manual Kafka Producer consoles use.

	kafka-console-producer.bat --broker-list 127.0.0.1:9092 --topic test
	kafka-console-producer.bat --broker-list 127.0.0.1:9092 --topic test2

To open the Zookeeper shell use

	zookeeper-shell 127.0.0.1:2181
	
To see the details of the znode use 

	ls /Spark
	get /Spark/test -> will result in the value assigned to it.

## To run the Program 

### In Eclipse/Java, pass the following in arguments :

	127.0.0.1:9092 test,test2 Spark
	
	
**Please leave your comments in there for more answers or feedbacks**

Chalo ae pyara azizo chalo.

biradar tu nasihat sun, haqiqat na gulo tu chun, zameen this aasmani ban, bhalu boi bhalu tu lan, hidayat no tu le rasto,firishta si tu kar rishto