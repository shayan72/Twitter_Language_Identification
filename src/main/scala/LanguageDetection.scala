import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object LanguageDetection {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

//    val sc = new SparkContext(conf)

    // Spark Streaming Configuration
    val conf = new SparkConf().setAppName("LanguageDetection").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1)) // Spark Streaming Context, Batch Every 1 Second

    // Kafka Configuration
    val kafkaParams = Map("bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Set("twitter-topic")

    // Spark Streaming and Kafka Integration
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)) // ConsumerRecord

//    val wordCounts = stream.count()
//    wordCounts.print()

    stream.foreachRDD { rdd =>
      val tweet_rdd = rdd.map(line => line.value).flatMap{ line =>
        var ngrams = TextProcessing.create_ngrams(line, 3)
        ngrams
      }

      tweet_rdd.collect().foreach(println)
    }

    ssc.start() // Start Streaming Thread

//    // Twitter Stream
    TwitterStreamingAPI.streamingClient.sampleStatuses(stall_warnings = true)(TwitterStreamingAPI.TweetTextToKafka("twitter-topic"))

    ssc.awaitTermination()
  }
}
