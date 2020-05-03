package org.streaming

import java.io.IOException
import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


object KafkaSparkProcessor {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[6]")
    .appName("Twitter Processor")
    .getOrCreate()
  spark.conf.set("spark.executor.memory", "5g")

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._
  import spark.sql

  trait SENTIMENT_TYPE
  case object VERY_NEGATIVE extends SENTIMENT_TYPE
  case object NEGATIVE extends SENTIMENT_TYPE
  case object NEUTRAL extends SENTIMENT_TYPE
  case object POSITIVE extends SENTIMENT_TYPE
  case object VERY_POSITIVE extends SENTIMENT_TYPE
  case object NOT_UNDERSTOOD extends SENTIMENT_TYPE

  val nlpProps: Properties = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  def main(args: Array[String]): Unit = {

    sc.setLogLevel("WARN")

    if (args.length != 1)  {
      println("Usage: KafkaSparkProcessor <topic-name>")
      return ;
    }
    // Pass kafka topic name
    val Array(topics) = args

    // Set the Spark StreamingContext to create a DStream for every 30 seconds
    val ssc = new StreamingContext(sc, Seconds(30))
    ssc.checkpoint("checkpoint")

    // Setup a stream to read messages from the Kafka topic
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topicSet = topics.split(",").toSet

    val stream = KafkaUtils.
      createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topicSet, kafkaParams))
    val tweets = stream.map(record => record.value).cache()

    //val hashTagCountRDD = getHashTagCounts(tweets).cache()
    //val htInfoRDD = hashTagCountRDD.join(hashTagSentimentRDD)
    val hashTagSentimentRDD = processTweet(tweets).cache()

    // Create a data frame and write to database
    val schema = new StructType()
      .add(StructField("timestamp", IntegerType, nullable = true))
      .add(StructField("hashtag", StringType, nullable = true))
      .add(StructField("sentiment-score", DoubleType, nullable = true))
      .add(StructField("sentiment-type", StringType, nullable = true))
      .add(StructField("country", StringType, nullable = true))

    var counter = 0
    hashTagSentimentRDD.foreachRDD((rdd: RDD[(String, Double, String, String)],
                                    time: org.apache.spark.streaming.Time) => {
      try {
        val newRDD = rdd.map(r =>
          Row(( time.milliseconds / 1000).toInt, r._1, r._2, r._3, r._4))
        val df = spark.createDataFrame(newRDD, schema).cache()
        val process_df = df.dropDuplicates(Seq("timestamp", "hashtag", "location"))
        process_df.show()
        writeToMySQL(process_df, counter)
        counter += 1
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
    })
    tweets.count().map(cnt => "Received " + cnt + " kafka messages.").print()
    ssc.start()
    ssc.awaitTermination()
  }

  def getHashTagCounts(tweets: DStream[String]): DStream[(String, Int)] = {

    val wordsInTweet = tweets.flatMap(tweet => tweet.split(" "))
    val hashTags = wordsInTweet.filter(word => word.startsWith("#"))
    val cleanHT = hashTags.map(tag => tag.replaceAll("\\s\t\n:,!", ""))

    // Get the top hashtags over the previous 60/10 sec window
    val hashTagCountRDD = cleanHT.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
    hashTagCountRDD
  }

  def _detectSentiment(message: String): (Double, SENTIMENT_TYPE with Product with Serializable) = {

    val pipeline = new StanfordCoreNLP(nlpProps)
    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = sentence.toString

      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }

      sentiments += sentiment.toDouble
      sizes += partText.length
    }

    val averageSentiment:Double = {
      if(sentiments.nonEmpty) sentiments.sum / sentiments.size
      else -1
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / sizes.sum

    if(sentiments.isEmpty) {
      mainSentiment = -1
      weightedSentiment = -1
    }
    /*
     0 -> very negative
     1 -> negative
     2 -> neutral
     3 -> positive
     4 -> very positive
     */
    val sentimentScore = weightedSentiment
    val sentimentType = weightedSentiment match {
      case s if s <= 0.0 => NOT_UNDERSTOOD
      case s if s < 1.0 => VERY_NEGATIVE
      case s if s < 2.0 => NEGATIVE
      case s if s < 3.0 => NEUTRAL
      case s if s < 4.0 => POSITIVE
      case s if s < 5.0 => VERY_POSITIVE
      case s if s > 5.0 => NOT_UNDERSTOOD
    }
    (sentimentScore, sentimentType)
  }

  def _getCountryInfo(location: String): String = {
    var country = "NULL"
    if (location.contains(",")) {
      val locObj = location.split(",")
      country = locObj(locObj.length - 1).replaceAll("\\s", "")
      if (country.length == 2) {
        country = "USA"
      }
    }
    country
  }

  def processTweet(tweets: DStream[String]): DStream[(String, Double, String, String)] = {

    val metricsStream = tweets.flatMap { eTweet => {
      val retList = ListBuffer[String]()
      for (tag <- eTweet.split(" ")) {
        if (tag.startsWith("#") && tag.replaceAll("\\s", "").length > 1) {
          val tweetObj = eTweet.split(" /TLOC/ ")
          val country = _getCountryInfo(tweetObj(0))
          val tweet_clean = tweetObj(1)
            .replaceAll("(\\b\\w*RT)|[^a-zA-Z0-9\\s.,!@]", "")
            .replaceAll("(http\\S+)", "")
            .replaceAll("(@\\w+)", "Foo")
            .replaceAll("^(Foo)", "")
          try {
            val (sentimentScore, sentimentType) = _detectSentiment(tweet_clean)
            retList += (tag + " /TLOC/ " + sentimentScore + " /TLOC/ " +
              sentimentType.toString.toLowerCase + " /TLOC/ " + country)

          } catch {
            case e: IOException => e.printStackTrace(); (tag, "-1.0")
          }
        }
      }
      retList.toList
    }}
    val processedTweet = metricsStream.map(line => {
      val Array(tag, sentiScore, sentiType, location) = line.split(" /TLOC/ ")
      (tag.replaceAll("(\\w*RT)|[^a-zA-Z0-9#]", ""),
        sentiScore.toDouble, sentiType, location)
    })
    /*
    // averaging the sentiment for each hash tag (not being used at the moment)
    val htSenti = processedTweet.mapValues(value => (value, 1)).reduceByKey {
      case ((sumL, countL), (sumR, countR)) =>
        (sumL + sumR, countL + countR)
    }.mapValues {
      case (sum , count) => sum / count
    }
    */
    processedTweet
  }

  def writeToMySQL(df: DataFrame, counter: Int): Unit = {

    // define jdbc connection parameters
    val tableName = "realtime_trends"
    val url = "jdbc:mysql://localhost:3306/twitter?serverTimezone=UTC"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "password")
    properties.setProperty("JDBC_TXN_ISOLATION_LEVEL", "READ_COMMITTED")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // drop table if exists
    if (counter == 0) {
      val conn = DriverManager.getConnection(url, "root", "password")
      println("Deleting table in given database...")
      val stmt = conn.createStatement
      val sqlDrop = "DROP TABLE IF EXISTS realtime_trends"
      stmt.executeUpdate(sqlDrop)
      println("Table deleted in given database...")
      conn.close()
    }
    println(df.rdd.partitions.length)
    // write or append data to MySql table
    df.repartition(10).write.mode(saveMode = "append").jdbc(url, tableName, properties)
  }
}
