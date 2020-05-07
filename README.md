# Real-Time Data Pipeline for Twitter Trends Analysis

![](https://github.com/akshitvjain/realtime-twitter-trends-analytics/blob/master/images/realtime-twitter-dashboard.gif)

## Motivation

In today’s era, the analysis of real-time data is becoming critical for SMEs & Large Corporations alike. Industries such as Financial services, Legal services, IT operation management services, Marketing and Advertising all require the analysis of massive amounts of real-time data as well as historical data in order to make business decisions.

Big data is defined by velocity, volume, and variety of the data; these characteristics make Big data different from
regular data. Unlike regular big data applications, real-time data processing applications require building a distributed data pipeline for capturing, processing, storing, and analyzing the data efficiently.

This personal project is a means for me to apply the theory of large-scale parallel data processing (CS 6240 - NEU), to build a real-time processing pipeline using open source tools that can capture large amounts of data from various data sources, process, store, and analyze the large-scale data efficiently. 

## Project Description

Twitter streaming trends popularity and sentiment analysis is an excellent choice for building a distributed data pipeline. Every day around 500 million tweets (as of October, 2019) are produced from all over the world, and around 1% of them are publicly available, that is 5 millions tweets. 

The data pipeline uses <b>Apache Kafka</b> as a data ingestion system, <b>Apache Spark</b> as a real-time data processing system, <b>MongoDB</b> for distributed storage and retrieval, and <b>Apache Drill</b> to connect MongoDB with <b>Tableau</b> for real-time analytics.

The Twitter data is acquired using Twitter Streaming API and is streamed to Kafka which makes it available for Spark that performs data processing and sentiment classification and stores the results into MongoDB. The popularity and sentiment of the trends are analyzed through a Tableau dashboard.

<b>Note:</b> Apache Drill connects MongoDB with Tableau. More on Drill later.

## Data Architecture

![link](https://github.com/akshitvjain/realtime-twitter-trends-analytics/blob/master/images/pipeline-architecture.png)

Kafka twitter streaming producer publishes streaming tweets to the ‘tweets-1’ topic in an Apache Kafka broker; the Apache Spark Streaming Context is subscribed to read the tweets from the 'tweets-1' topic. The Spark engine leverages Spark Streaming to perform batch processing on incoming tweets, and performs sentiment classification before storing the processed results in the MongoDB. Drill connects MongoDB to Tableau; the realtime data is used to create a live dashboard to analyze popularity and sentiment of trending topics on Twitter.

## System Design

The different components of the data pipeline, Kafka Twitter Streaming Producer, Apache Kafka, Apache Spark Streaming, MongoDB, Apache Drill and Tableau are all run locally for development.

<b> Kafka Twitter Streaming Producer: </b>

Is a Kafka producer used for publishing streaming tweets to central Apache Kafka on topic ‘tweets-1’ in real time from all over the world in English by using twitter4j library for twitter API.

<b> Apache Kafka: </b>

Apache Kafka is a distributed publish-subscribe messaging system and a robust queue that can handle a high volume of data and enables you to pass messages from one end-point to another. Kafka is suitable for both offline and online message consumption. Kafka messages are persisted on the disk and replicated within the cluster to prevent data loss. It integrates very well with Apache Spark for real-time streaming data analysis.

A critical dependency of Apache Kafka is Apache Zookeeper, which is a distributed configuration and synchronization service. Zookeeper serves as the coordination interface between the Kafka brokers and consumers. The Kafka servers share information via a Zookeeper cluster. Kafka stores basic metadata in Zookeeper such as information about topics, brokers, consumer offsets (queue readers) and so on.

Since all the critical information is stored in Zookeeper and it normally replicates this data across its ensemble, failure of Kafka broker/Zookeeper does not affect the state of the Kafka cluster. Kafka will restore the state, once Zookeeper restarts. This gives zero downtime for Kafka. The leader election between the Kafka broker is also done by using Zookeeper in the event of leader failure.

This project was configured to have one kafka broker and one zookeeper instance respectively. It is mainly used as a queue to publish raw streaming tweets for processing. It maintained one topic in this project, ‘tweets-1’.

More partitions in a Kafka cluster leads to higher throughput however, higher number of partitions have an impact on availability and latency as well. Therefore, it is best to run a single consumer over a topic with 5, 10, 15, ... partitions; measure the impact and pick the configuration that meets the requirements. At the end of the day, if you need more partitions you should not worry about re-balancing.

<b> Apache Spark: </b>

<i>Spark Core - </i>

Apache Spark is a fast and general-purpose distributed cluster computing framework. Spark core is the foundation of overall project. It supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, MLlib for machine learning, GraphX for graph processing, and Spark Streaming.

Spark’s primary data abstraction is a distributed collection of items called a Resilient Distributed Dataset (RDD). RDD represents an immutable, partitioned collection of elements that can be operated on in parallel with fault-tolerance.

<i>RDD has several traits:</i>

- Resilient, i.e. fault-tolerant with the help of RDD lineage graph and so able to recompute missing or damaged partitions due to node failures. Compared to Hadoop MapReduce, which persists data to disk after map or reduce action, Spark achieves fault-tolerant by replicating same block of data three times on different nodes. This is one of the reasons why Spark is faster than Hadoop MapReduce.

- Immutable or Read-Only, i.e. it does not change once created and can only be transformed using transformations to new RDDs.

- Lazy evaluated, i.e. the data inside RDD is not available or transformed until an action is executed that triggers the execution.

- Partitioned, i.e. the data inside a RDD is partitioned (split into partitions) and then distributed across nodes in a cluster. Partitions are the units of parallelism

<i>Spark Streaming - </i>

Spark streaming leverages spark core to perform streaming analysis. Discretized Stream or DStream is the basic abstraction provided by Spark Streaming. It represents a continuous stream of data, either the input data stream received from source, or the processed data stream generated by transforming the input stream. Internally, a DStream is represented by a continuous series of RDDs. Each RDD in a DStream contains data from a certain interval, as shown in the following figure.

![link](https://github.com/akshitvjain/realtime-twitter-trends-analytics/blob/master/images/dstream-rdd-abstraction.png)

Any operation applied on a DStream translates to operations on the underlying RDDs.

![link](https://github.com/akshitvjain/realtime-twitter-trends-analytics/blob/master/images/flatMap-on-dstream.png)

The major part of the data processing required a series of transformations on input of raw streaming tweets for sentiment classification. The transformation on DStreams can be grouped into either stateless or stateful.

Stateless transformations are simple RDD transformation being applied on every batch, that is, every RDD in a DStream, such as map(), flatMap(), filter(), reducedByKey() and so on. Stateless transformations were used to filter emoticons, hyperlinks and non alphanumeric characters in each tweet, map each tweet to tuple format of (timestamp, tag, sentiment-score, sentiment-type, country) before converting the stream of RDDs to a Spark Dataframe and writing to MongoDB.

Statefull transformation are operations on DStream that track data across time, that is, some data from previous batches is used to generate the results for a new batch.

<b> Apache Drill: </b>

Apache Drill is an open-source SQL execution engine that makes it possible to use SQL to query non-relational databases and file systems. This includes joins between these systems – for example, Drill could allow a user to join ecommerce data stored in Elasticsearch with web analytics data stored in a Hadoop cluster. It also supports data-intensive distributed applications for interactive analysis of large-scale datasets. 

In this project Drill is mainly used to integrate MongoDB with Tableau. 

## Instructions to Setup Data Pipeline and Dashboard
(Setup instructions for development and testing on local machine.)

1. Download [Zookeeper](https://www.apache.org/dyn/closer.lua/zookeeper/zookeeper-3.5.7/apache-zookeeper-3.5.7-bin.tar.gz), [MongoDB](https://docs.mongodb.com/guides/server/install/), [Apache Kafka](https://archive.apache.org/dist/kafka/2.4.0/kafka_2.12-2.4.0.tgz), [Apache Spark](https://spark.apache.org/downloads.html) and [Apache Drill](https://drill.apache.org/docs/installing-drill-on-linux-and-mac-os-x/)

2. [Insturctions to Setup Spark Development Environment](https://kaizen.itversity.com/setup-development-environment-intellij-and-scala-big-data-hadoop-and-spark/) (Optional).

3. Clone the project repository.

4. Create a Twiter developer account - More information can be found [here](https://developer.twitter.com/en/apply-for-access).

5. Update the oAuth-tokens.txt in the input directory with the respective Twitter API keys and tokens.

6. Start the zookeeper server: 

```bash
/usr/local/zookeeper/bin/zkServer.sh start
```

7. Start kafka server: 
```bash
/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties
```

8. Create a topic “tweets-1” in kafka:
```bash
/usr/local/kafka/bin/kafka-topics.sh –create –zookeeper localhost:2181 –replication-factor 1 –partitions 1 –topic tweets-1
```

9. Check if topic has been created: 
```bash
/usr/local/kafka/bin/kafka-topics.sh –list –zookeeper localhost:2181
```

10. Start MongoDB Server

11. Start Apache Drill daemon in distributed mode - More information can be found [here](https://drill.apache.org/docs/starting-drill-in-distributed-mode/)
```bash
bin/drillbit.sh start
```

12. Enable MongoDB Storage Plugin using Drill's WebUI console - More information can be found [here](https://drill.apache.org/docs/mongodb-storage-plugin/)

12. Run the KafkaTwitterProducer.java:

![link](https://github.com/akshitvjain/realtime-twitter-trends-analytics/blob/master/images/producer-args.png)

13. Next, run the KafkaSparkProcessor.scala:

![link](https://github.com/akshitvjain/realtime-twitter-trends-analytics/blob/master/images/spark-args.png)

14. Finally, configure and connect Tableau to MongoDB using Apache Drill - More information can be found [here](https://help.tableau.com/current/pro/desktop/en-us/examples_apachedrill.htm)

## Tools + IDE

- Apache Kafka 2.4.0
- Apache Spark 2.4.1
- Apache Drill 1.17.0
- MongoDB
- Tableau Desktop
- IntelliJ IDEA
- Java 8
- Scala 2.11.12

