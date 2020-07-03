import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

object AnalyzeStream {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Error: the class expects minimum 2 arguments.")
      println("Usage: TwitterHandler KafkaTopic TwitterFilter1 [more comma separated TwitterFilters]")
    }
    // Setting up configurations
    val sparkConfiguration = new SparkConf().
      setAppName("spark-streaming-with-twitter-and-kafka").
      setMaster(sys.env.getOrElse("spark.master", "local[*]"))

    val sparkContext = new SparkContext(sparkConfiguration)
    sparkContext.setLogLevel("ERROR")

    val slideInterval = new Duration(1 * 1000)

    val timeoutJobLength = 3600 * 1000

    val kafkaBrokers = "localhost:9092,localhost:9093"

    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val streamingContext = new StreamingContext(sparkContext, slideInterval)

    // Creating Tweets stream
    val tweetStream: DStream[Status] =
      TwitterUtils.createStream(streamingContext, None, args.slice(1, args.length))

    // Sentiment Analysis
    val tweets = tweetStream.map(x => (x.getText, SentimentAnalyzer.getSentiment(x.getText).toString))
    tweets.print()

    tweets.foreachRDD(rdd => {
      rdd.foreachPartition(f = partition => {
        val producer = new KafkaProducer[String, String](properties)

        partition.foreach(record => {
          // Sending tweet as Key and sentiment as value.
          val producerMessage = new ProducerRecord[String, String](args(0), record._1, record._2)
          println(producerMessage)
          producer.send(producerMessage)
        })

        producer.flush()
        producer.close()
      })
    })

    streamingContext.start()

    // await the stream to end - 3600 seconds wait
    streamingContext.awaitTerminationOrTimeout(timeoutJobLength)
  }
}
