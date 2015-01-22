import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.json4s.ShortTypeHints
import scala.util.Random

trait Tweets

case class TweetData(user: Int, content: String) extends Tweets

object Tweets {
  val tweetsList = List[Tweets](
    TweetData(user = Random.nextInt(100), content = Random.alphanumeric.take(Random.nextInt(140)).mkString)
  )

  private implicit val formats = Serialization.formats(ShortTypeHints(List(classOf[TweetData])))
  def toJson(tweetsList: List[Tweets]): String = writePretty(tweetsList)
  def toJson(tweets: Tweets): String = writePretty(tweets)
}