import org.apache.kafka._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaProducerTwitter {
  var producer = None: Option[KafkaProducer[String, String]]
  locally {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "ScalaSalamProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    producer = Some(new KafkaProducer[String, String](props))
  }

  def produce(tweet: String) = {
    val topic = "twitter-topic"
    val data = new ProducerRecord[String, String](topic, "localhost", tweet)
    producer.get.send(data)
  }
}
