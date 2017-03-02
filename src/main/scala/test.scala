import twitter4j._

object test {

  object Util {
    val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey("Rq2sVcaStrMhQLfyNML5KLZTq")
      .setOAuthConsumerSecret("SCZ7KVFlMJPQ2Nd6VSpCNsNOAazEQ6hvVntknAaATliSoFZQZX")
      .setOAuthAccessToken("17532913-seFecsf8EXYNvPwDzNc5ZF7OPYqlbVBoQ6Cl7TSya")
      .setOAuthAccessTokenSecret("mM6zQrx08K3LXVGmVUvKQkUrLmwDrjJHywrIGhRk7mCrP")
      .build

    def simpleStatusListener = new StatusListener() {
      def onStatus(status: Status) { println(status.getText) }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onException(ex: Exception) { ex.printStackTrace }
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    }
  }

  def main(args: Array[String]): Unit = {
    val twitterStream = new TwitterStreamFactory(Util.config).getInstance
    twitterStream.addListener(Util.simpleStatusListener)
    twitterStream.sample
//    Thread.sleep(10000)
//    twitterStream.cleanUp
//    twitterStream.shutdown
  }
}
