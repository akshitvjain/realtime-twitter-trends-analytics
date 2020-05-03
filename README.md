# Real-Time Data Pipeline for Twitter Trends Analysis

## Motivation

In today’s era, the analysis of real-time data is becoming critical for SMEs & Large Corporations alike. Industries such as Financial services, Legal services, IT operation management services, Marketing and Advertising, all require the analysis of massive amounts of real-time data as well as historical data in order to make business decisions.

Big data is defined by velocity, volume, and variety of the data; these characteristics make Big data different from
regular data. Unlike regular big data applications, real-time data processing applications require building a distributed data pipeline for capturing, processing, storing, and analyzing the data efficiently.

This personal project is a means for me to apply the theory of large-scale parallel data processing (CS 6240 - NEU), to build a real-time processing pipeline using open source tools that can capture a large amount of data from various data sources, process, store, and analyze the large-scale data efficiently. 

The data pipeline uses <b>Apache Kafka</b> as data ingestion system, <b>Apache Spark</b> as a real-time data processing system, <b>MongoDB</b> for distributed storage, and <b>Tableau</b> for creating dashboard for data analysis.

## Project Description

Twitter streaming trends popularity and sentiment analysis is an excellent choice for building a distributed data pipeline. Every day around 500 million tweets (as of October, 2019) are produced from all over the world, and around 1% of them are publicly available, that is 5 millions tweets. 

The Twitter data is acquired using Twitter Streaming API and is streamed to Kafka which makes it available for Spark that performs data processing and sentiment classification and stores the results into MongoDB. The popularity and sentiment of the trends are analyzed through a Tableau dasboard.

## Data Architecture

![link](https://github.com/akshitvjain/realtime-twitter-trends-analytics/blob/master/images/pipeline-architecture.png)

Kafka twitter streaming producer publishes streaming tweets to the ‘tweets-1’ topic in an Apache Kafka broker; the Apache Spark Streaming Context is subscribed to read the tweets from the 'tweets-1' topic. The Spark engine leverages Spark Streaming to perform batch processing on incoming tweets, and performs sentiment classification before storing the processed results in MongoDB. Tableau connects to MongoDB to retrieve the results and creates a live dashboard to analyze popularity and sentiment of trending topics on Twitter.








## System Design
## Setup Instructions
## Tools
## Links
