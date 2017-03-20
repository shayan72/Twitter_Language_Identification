import java.util.regex.{Matcher, Pattern}

/**
  * Created by shayansalehian on 3/8/17.
  */
object TextProcessing {
  def create_ngrams(line : String, n: Int) : Array[String] = {
    removeUrl(line).toLowerCase.replaceAll("""[\p{Punct}&&[^.]]""", "").split("[\\p{Punct}\\s]+").flatMap(word => word.sliding(n))
  }

  def removeUrl(string: String) : String = {
    var commentstr = string
    var urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$~_?\\+-=\\\\\\.&]*)"
    var p = Pattern.compile(urlPattern, Pattern.CASE_INSENSITIVE)
    var m = p.matcher(commentstr)
    var i = 0
    while (m.find()) {
      commentstr = commentstr.replaceAll(m.group(i),"").trim()
      i += 1
    }

    commentstr
  }
}
