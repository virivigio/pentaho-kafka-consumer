pentaho-kafka-consumer
======================

Apache Kafka consumer step plug-in for Pentaho Kettle.
Forked from [Michael Spector](https://github.com/spektom)'s [RuckusWirelessIL/pentaho-kafka-consumer](https://github.com/RuckusWirelessIL/pentaho-kafka-consumer)


Adds:
 - timestamp
 - partition
 - offset
 
information. But now requires Kafka 10.0.1.0. For version 0.8.1.1 go to the original project.

### Screenshots ###

![Using Apache Kafka Consumer in Kettle](https://raw.github.com/RuckusWirelessIL/pentaho-kafka-consumer/master/doc/example.png)


### Apache Kafka Compatibility ###

The consumer depends on Apache Kafka 0.8.1.1, which means that the broker must be of 0.8.x version or later.

If you want to build the plugin for a different Kafka version you have to
modify the values of kafka.version and kafka.scala.version in the properties
section of the pom.xml. 

### Maximum Duration Of Consumption ###

Note that the maximum duration of consumption is a limit on the duration of the
entire step, *not* an individual read. This means that if you have a maximum
duration of 5000ms, your transformation will stop after 5s, whether or
not more data exists and independent of how fast each message is fetched from
the topic. If you want to stop reading messages when the topic has no more
messages, see the section on _Empty topic handling_.

### Empty topic handling ###

If you want the step to halt when there are no more messages available on the
topic, check the "Stop on empty topic" checkbox in the configuration dialog. The
default timeout to wait for messages is 1000ms, but you can override this by
setting the "consumer.timeout.ms" property in the dialog. If you configure a
timeout without checking the box, an empty topic will be considered a failure
case.

### Installation ###

1. Download ```pentaho-kafka-consumer``` Zip archive from [latest release page](https://github.com/RuckusWirelessIL/pentaho-kafka-consumer/releases/latest).
2. Extract downloaded archive into *plugins/steps* directory of your Pentaho Data Integration distribution.


### Building from source code ###

```
mvn clean package
```
