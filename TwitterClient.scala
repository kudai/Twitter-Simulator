import akka.actor.{ Actor, ActorRef, Props, ActorSystem }
import akka.actor.actorRef2Scala
import akka.dispatch.ExecutionContexts.global
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration.DurationInt
import scala.io.Source.fromFile
import java.security.MessageDigest
import java.util.UUID
import akka.actor.{ Address, AddressFromURIString }
import akka.routing.RoundRobinRouter
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.mutable._
import java.io._
import spray.http._
import spray.client.pipelining._
import akka.actor.ActorSystem
import scala.util.Random
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.json4s.ShortTypeHints
import org.json4s.Formats
import spray.routing._
import spray.http.MediaTypes
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import Tweets._
import spray.httpx._

case class Inilialize()
case class START()
case class Tweet(i: Int)
case class Stop(start: Long, i: Int)
case class users(num: Int)
case class Retrieve(num: Int)
case class Retrieve_Self(num: Int)
case class DeleteTweet(num: Int)
case class Retweet(num: Int)
case class Final(count: Int)
case class CheckStart(Start: Long)
case class AddUser()
case class RemoveUser(num: Int)
case class FollowUser(num: Int)
case class UnfollowUser(num: Int)

object TwitterClient {
  //System.setOut(new PrintStream(new FileOutputStream("output.doc")));
  def main(args: Array[String]) {
    val system = ActorSystem("RESTClientSystem")
    val Initializer = system.actorOf(Props(new Initializer((args(0).toInt), args(1), system)), name = "Starter")
    Initializer ! Inilialize()
  }
}

class Initializer(UserCount: Int, IP: String, system: ActorSystem) extends Actor {
  var ClientActor = new ArrayBuffer[ActorRef]()
  import system.dispatcher
  val pipeline2 = sendReceive
  val securePipeline2 = addCredentials(BasicHttpCredentials("kush", "passwd")) ~> sendReceive
  val result = securePipeline2(Post("http://" + IP + ":8080/list/all"))
  result.foreach { response =>
    println(s"Request completed with status ${response.status} and content:\n${response.entity.asString}")
  }

  def receive =
    {
      case Inilialize() =>
        {
          val start: Long = System.currentTimeMillis
          var ClientActor1 = context.actorOf(Props(new Client(IP, system)), name = "checkstartactor")
          pipeline2(Post("http://" + IP + ":8080/tweet/total?numUser=" + UserCount))
          var duration: Duration = (System.currentTimeMillis - start).millis
          var duration1 = (System.currentTimeMillis - start).millis
          while (duration1 < 5.second) {
            duration1 = (System.currentTimeMillis - start).millis
          }
          self ! START()
        }

      case START() =>
        {
          val start: Long = System.currentTimeMillis
          val cores: Int = (Runtime.getRuntime().availableProcessors()) * 8
          for (i <- 0 until cores) {
            ClientActor += context.actorOf(Props(new Client(IP, system)), name = "Client" + i)
          }
          for (i <- 0 until cores) {
            import system.dispatcher
            system.scheduler.schedule(0 milliseconds, 100 milliseconds, ClientActor(i), Stop(start, UserCount))
          }
          for (i <- 0 to UserCount - 1) {
            import system.dispatcher
            system.scheduler.schedule(0 milliseconds, 10 milliseconds, ClientActor(i % cores), Tweet(i))
          }

          import system.dispatcher
          system.scheduler.schedule(100 milliseconds, 100 milliseconds, ClientActor(Random.nextInt(cores - 1)), Retweet(UserCount))

          import system.dispatcher
          system.scheduler.schedule(100 milliseconds, 300 milliseconds, ClientActor(Random.nextInt(cores - 1)), DeleteTweet(UserCount))

          import system.dispatcher
          system.scheduler.schedule(100 milliseconds, 500 milliseconds, ClientActor(Random.nextInt(cores - 1)), FollowUser(UserCount))

          import system.dispatcher
          system.scheduler.schedule(100 milliseconds, 500 milliseconds, ClientActor(Random.nextInt(cores - 1)), UnfollowUser(UserCount))

          import system.dispatcher
          system.scheduler.schedule(0 milliseconds, 1000 milliseconds, ClientActor(Random.nextInt(cores - 1)), Retrieve(UserCount))

          import system.dispatcher
          system.scheduler.schedule(0 milliseconds, 1000 milliseconds, ClientActor(Random.nextInt(cores - 1)), Retrieve_Self(UserCount))

          import system.dispatcher
          system.scheduler.schedule(0 milliseconds, 1000 milliseconds, ClientActor(Random.nextInt(cores - 1)), AddUser())

          import system.dispatcher
          system.scheduler.schedule(100 milliseconds, 2000 milliseconds, ClientActor(Random.nextInt(cores - 1)), RemoveUser(UserCount))

        }
    }
}

class Client(IP: String, system: ActorSystem) extends Actor {
  import system.dispatcher
  val pipeline1 = sendReceive
  val securePipeline1 = addCredentials(BasicHttpCredentials("kush", "passwd")) ~> sendReceive
  val result = securePipeline1(Post("http://" + IP + ":8080/list/all"))
  result.foreach { response =>
    println(s"Request completed with status ${response.status} and content:\n${response.entity.asString}")
  }

  def receive =
    {
      case Tweet(i) =>
        {
          pipeline1(Post("http://" + IP + ":8080/tweet/new?user=" + i + "&content=" + Random.alphanumeric.take(140).mkString))
        }

      case Retweet(num) =>
        {
          pipeline1(Post("http://" + IP + ":8080/tweet/retweet?user=" + Random.nextInt(num)))
        }

      case Retrieve(num) =>
        {
          pipeline1(Post("http://" + IP + ":8080/tweet/User?UserID=" + Random.nextInt(num - 1)))
        }

      case Retrieve_Self(num) =>
        {
          pipeline1(Post("http://" + IP + ":8080/tweet/User_Self?UserID=" + Random.nextInt(num - 1)))
        }
      
      case DeleteTweet(num) =>
        {
          pipeline1(Post("http://" + IP + ":8080/tweet/delete?user=" + Random.nextInt(num) + "&tweetID=" + Random.nextInt(10)))
        }

      case AddUser() =>
        {
          pipeline1(Post("http://" + IP + ":8080/user/add"))
        }

      case RemoveUser(num) =>
        {
          pipeline1(Post("http://" + IP + ":8080/user/delete?user=" + Random.nextInt(num - 1)))
        }

      case FollowUser(num) =>
        {
          pipeline1(Post("http://" + IP + ":8080/user/follow?user=" + Random.nextInt(num - 1) + "&tofollow=" + Random.nextInt(num - 1)))
        }

      case UnfollowUser(num) =>
        {
          pipeline1(Post("http://" + IP + ":8080/user/unfollow?user=" + Random.nextInt(num - 1) + "&tounfollow=" + Random.nextInt(num - 1)))
        }

      case Stop(start, i) =>
        {
          val duration: Duration = (System.currentTimeMillis - start).millis
          if (duration > 60.second) {
            // remote ! GetCount()
            pipeline1(Post("http://" + IP + ":8080/tweet/Stop?Counter=" + 1))
            context.stop(self)
            //System.exit(0)
          }
        }
    }
}
