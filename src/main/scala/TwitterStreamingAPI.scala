import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.Tweet
import com.danielasfregola.twitter4s.entities.streaming.StreamingMessage


object TwitterStreamingAPI {

  val restClient = TwitterRestClient()
  val streamingClient = TwitterStreamingClient()

  def printTweetText: PartialFunction[StreamingMessage, Unit] = {
    case tweet: Tweet =>
      println(tweet.text)
      println("================================")
  }

  def TweetTextToKafka(topic: String): PartialFunction[StreamingMessage, Unit] = {
    case tweet: Tweet => KafkaProducer.produce(tweet.text, topic)
  }

  def TweetTextLangToKafka(topic: String): PartialFunction[StreamingMessage, Unit] = {
    case tweet: Tweet => KafkaProducer.produce(tweet.lang.get + ", " + tweet.text, topic)
  }

}
