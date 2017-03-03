import twitter4j._
import kafka._
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.log4j.Logger
import org.apache.log4j.Level

object test {

  object Util {
    val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey("Rq2sVcaStrMhQLfyNML5KLZTq")
      .setOAuthConsumerSecret("SCZ7KVFlMJPQ2Nd6VSpCNsNOAazEQ6hvVntknAaATliSoFZQZX")
      .setOAuthAccessToken("17532913-seFecsf8EXYNvPwDzNc5ZF7OPYqlbVBoQ6Cl7TSya")
      .setOAuthAccessTokenSecret("mM6zQrx08K3LXVGmVUvKQkUrLmwDrjJHywrIGhRk7mCrP")
      .build

    def simpleStatusListener = new StatusListener() {
      def onStatus(status: Status) {
        //          println(status.getText)
        KafkaProducerTwitter.produce(status.getText)

        //        // Spark test
        //        val logFile = "README.md" // Should be some file on your system
        //        val conf = new SparkConf().setAppName("Twitter Language Identification").setMaster("local[2]").set("spark.executor.memory","1g")
        //        val sc = new SparkContext(conf)
        //        val logData = sc.textFile(logFile, 2).cache()
        //        val numAs = logData.filter(line => line.contains("a")).count()
        //        val numBs = logData.filter(line => line.contains("b")).count()
        //        println(s"Lines with a: $numAs, Lines with b: $numBs")
        //        sc.stop()
      }

      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}

      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}

      def onException(ex: Exception) {
        ex.printStackTrace
      }

      def onScrubGeo(arg0: Long, arg1: Long) {}

      def onStallWarning(warning: StallWarning) {}
    }
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Twitter Stream
    val twitterStream = new TwitterStreamFactory(Util.config).getInstance
    twitterStream.addListener(Util.simpleStatusListener)
    twitterStream.sample
    //    Thread.sleep(10000)
    //    twitterStream.cleanUp
    //    twitterStream.shutdown


    //    // Spark test
    //        val logFile = "README.md" // Should be some file on your system
    //        val conf = new SparkConf().setAppName("Twitter Language Identification").setMaster("local[2]").set("spark.executor.memory","1g")
    //        val sc = new SparkContext(conf)
    //        val logData = sc.textFile(logFile, 2).cache()
    //        val numAs = logData.filter(line => line.contains("a")).count()
    //        val numBs = logData.filter(line => line.contains("b")).count()
    //        println(s"Lines with a: $numAs, Lines with b: $numBs")
    //    sc.stop()

    // Spark Streaming + Kafka Integration
    val ssc = new StreamingContext("local[*]", "test", Seconds(1))

    val kafkaParams = Map("bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = List("twitter-topic").toSet
    val lines = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val wordCounts = lines.count()
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
