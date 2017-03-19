/**
  * Created by shayansalehian on 3/8/17.
  */
object TextProcessing {
  def create_bigrams(line : String) : Array[String] = {
    line.split(" ").flatMap(word => word.sliding(2))
  }
  def create_trigrams(line : String) : Array[String] = {
    line.split(" ").flatMap(word => word.sliding(3))
  }
}
