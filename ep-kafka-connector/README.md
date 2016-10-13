# Kafka Commands

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


zookeeper-shell 127.0.0.1:2181

biradar tu nasihat sun, haqiqat na gulo tu chun, zameen this aasmani ban, bhalu boi bhalu tu lan, hidayat no tu le rasto,firishta si tu kar rishto
Chalo ae pyara azizo chalo.