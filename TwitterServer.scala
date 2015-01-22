import akka.actor.{ Actor, ActorRef, Props, ActorSystem }
import akka.actor.actorRef2Scala
import akka.dispatch.ExecutionContexts.global
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration.DurationInt
import scala.io.Source.fromFile
import java.security.MessageDigest
import akka.routing.RoundRobinRouter
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import akka.dispatch.ExecutionContexts._
import scala.collection.mutable._
import scala.util.Random
import java.io._
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
import spray.http._
import spray.client.pipelining._
import akka.actor.ActorSystem
import scala.util.Random

case class CreateUsersTable(n: Int)
case class GetCount()
case class Tweet(ID: Int, tweet: String)
case class DeleteTweet(u: Int, v: Int)
case class Retweet(u: Int, v: Int)
case class AddUser()
case class RemoveUser(n: Int)
case class FollowUser(u: Int, v: Int)
case class UnfollowUser(u: Int, v: Int)
case class MyHomepage(n: Int)
case class MyTimeline(n: Int)

object TwitterServer extends App with SimpleRoutingApp {
  var tweetCount: Int = 0
  var numberOfUsers = 0
  def increaseTweetCount() {
    tweetCount = tweetCount + 1
  }
  override def main(args: Array[String]) {
    implicit val system = ActorSystem("TwitterSystem")
    val cores: Int = (Runtime.getRuntime().availableProcessors()) * 4
    var tweetListByUser = new ListBuffer[ListBuffer[String]]()
    var followingTable = new ArrayBuffer[ArrayBuffer[Int]]()

    println("Intitializing.\nPlease wait...\n")
    val serverRouter = system.actorOf(Props(new ServerActor(tweetListByUser, followingTable, system)).withRouter(RoundRobinRouter(cores)), name = "ServerActor")
    println(serverRouter)
    var allTweets = Tweets.tweetsList

    def getJson(route: Route) = get {
      respondWithMediaType(MediaTypes.`application/json`) { route }
    }

    startServer(interface = "localhost", port = 8080) {
      get {
        path("") { ctx =>
          ctx.complete("Welcome to Twitter!")
          println("Started Server!")
        }
      } ~
        getJson {
          path("list" / "all") {
            complete {
              Tweets.toJson(allTweets)
            }
          }
        } ~
        getJson {
          path("tweet" / IntNumber / "details") { index =>
            complete {
              Tweets.toJson(allTweets(index))
            }
          }
        } ~
        post {
          path("tweet" / "new") {
            parameters("user".as[Int], "content".as[String]) { (user, content) =>
              serverRouter ! Tweet(user, content)
              val newTweet = TweetData(user, content)
              allTweets = newTweet :: allTweets
              complete {
                "OK" + content
              }
            }
          }
        } ~
        post {
          path("tweet" / "retweet") {
            parameters("user".as[Int]) { (user) =>
              serverRouter ! Retweet(user, followingTable(user)(Random.nextInt(followingTable(user).size)))
              complete {
                "OK"
              }
            }
          }
        } ~
        post {
          path("tweet" / "delete") {
            parameters("user".as[Int], "tweetID".as[Int]) { (user, tweetID) =>
              serverRouter ! DeleteTweet(user, tweetID)
              complete {
                "OK"
              }
            }
          }
        } ~
        post {
          path("tweet" / "total") {
            parameters("numUser".as[Int]) { (numUser) =>
              serverRouter ! CreateUsersTable(numUser)
              complete {
                "number of users=" + numUser
              }
            }
          }
        } ~
        post {
          path("tweet" / "User") {
            parameters("UserID".as[Int]) { (UserID) =>
              serverRouter ! MyHomepage(UserID)
              complete {
                "UserID=" + UserID
              }
            }
          }
        } ~
        post {
          path("tweet" / "Stop") {
            parameters("Counter".as[Int]) { (Counter) =>
              serverRouter ! GetCount()
              complete {
                "Stop Counter=" + Counter
              }
            }
          }
        } ~
        post {
          path("tweet" / "User_Self") {
            parameters("UserID".as[Int]) { (UserID) =>
              serverRouter ! MyTimeline(UserID)
              complete {
                "UserID_Self=" + UserID
              }
            }
          }
        } ~
        post {
          path("user" / "add") { ctx =>
            serverRouter ! AddUser()
            ctx.complete("OK")
          }
        } ~
        post {
          path("user" / "delete") {
            parameters("user".as[Int]) { (user) =>
              serverRouter ! RemoveUser(user)
              complete {
                "OK"
              }
            }
          }
        } ~
        post {
          path("user" / "follow") {
            parameters("user".as[Int], "tofollow".as[Int]) { (user, tofollow) =>
              serverRouter ! FollowUser(user, tofollow)
              complete {
                "OK"
              }
            }
          }
        } ~
        post {
          path("user" / "unfollow") {
            parameters("user".as[Int], "tounfollow".as[Int]) { (user, tounfollow) =>
              serverRouter ! UnfollowUser(user, tounfollow)
              complete {
                "OK"
              }
            }
          }
        }
    }
  }

  class ServerActor(tweetListByUser: ListBuffer[ListBuffer[String]], followingTable: ArrayBuffer[ArrayBuffer[Int]], system: ActorSystem) extends Actor {
    var PRINTON = true
    def receive =
      {
        case CreateUsersTable(n) =>
          {
            println("Creating Following list.\n")
            numberOfUsers = n
            for (i <- 0 to numberOfUsers - 1) {
              val Q = new ListBuffer[String]
              tweetListByUser += Q
            }
            for (k <- 0 to numberOfUsers - 1) {
              val F = new ArrayBuffer[Int]
              followingTable += F
            }
            for (i <- 0 to numberOfUsers - 1) {
              var fol = Random.nextInt(numberOfUsers / 10)
              for (j <- 0 to fol) {
                var ran = Random.nextInt(numberOfUsers - 1)
                while (ran == i || followingTable(i).contains(ran)) {
                  ran = Random.nextInt(numberOfUsers - 1)
                }
                followingTable(i) += ran
              }
            }
            println(followingTable)
          }

        case GetCount() =>
          {
            println("Total Tweets = " + TwitterServer.tweetCount)
          }

        case Tweet(i, tweet) =>
          {
            tweetListByUser(i) += tweet
            TwitterServer.increaseTweetCount()
            if (tweetListByUser(i).length == 101)
              tweetListByUser(i).remove(0)
          }

        case Retweet(u, v) =>
          {
            if (!followingTable(u).isEmpty) {
              var t = tweetListByUser(v)(Random.nextInt(tweetListByUser(v).size - 1))
              var tempActor = system.actorOf(Props(new ServerActor(tweetListByUser, followingTable, system)))
              tempActor ! Tweet(u, t)
              if(PRINTON)
                println("User " + u + " retweeted user " + v)
            }
          }
        
        case DeleteTweet(u, n) =>
          {
            if (tweetListByUser(u).length > n) {
              tweetListByUser(u).remove(n)
              tweetCount = tweetCount - 1
              println("Deleted Tweet by User " + u)
            } else
              println("No such tweet exists")
          }
          
        case AddUser() =>
          {
            val F = new ArrayBuffer[Int]
            followingTable += F
            val Q = new ListBuffer[String]
            tweetListByUser += Q
            if (numberOfUsers > 10) {
              var fol = Random.nextInt(numberOfUsers / 10)
              for (j <- 0 to fol) {
                var ran = Random.nextInt(numberOfUsers)
                while (ran == numberOfUsers || followingTable(numberOfUsers).contains(ran)) {
                  ran = Random.nextInt(numberOfUsers - 1)
                }
                followingTable(numberOfUsers) += ran
              }
              numberOfUsers = numberOfUsers + 1
            }
            if(PRINTON)
              println("Added User " + numberOfUsers)
          }

        case RemoveUser(n) =>
          {
            if (n < numberOfUsers) {
              followingTable.remove(n)
              tweetListByUser.remove(n)
              numberOfUsers = numberOfUsers - 1
              for (i <- 0 to numberOfUsers - 1) {
                if (followingTable(i).contains(n))
                  followingTable(i) = followingTable(i).-(n)
              }
              if(PRINTON)
                println("Removed User" + (numberOfUsers + 1))
            } 
            else if(PRINTON)
              println("No such user exists.")
          }

        case FollowUser(u, v) =>
          {
            if (!followingTable(u).contains(v)) {
              followingTable(u) += v
              if(PRINTON)
                println("User " + u + " now follows user " + v)
            } 
            else if(PRINTON)
              println("User " + u + " already follows user " + v)
          }

        case UnfollowUser(u, v) =>
          {
            if (followingTable(u).contains(v)) {
              followingTable(u) = followingTable(u).-(v)
              if(PRINTON)
                println("User " + u + " unfollowed user " + v)
            } 
            else if(PRINTON)
              println("User " + u + " does not follow user " + v)
          }

        case MyHomepage(n) =>
          {
            if(followingTable.size > n)
            {
              for (k <- 0 to (followingTable(n).size) - 1) {
                if (!tweetListByUser(followingTable(n)(k)).isEmpty && PRINTON)
                  print("Tweets User "+n+" Follows: "+followingTable(n)(k)+" :"+tweetListByUser(followingTable(n)(k)))
              }
            }
          }

        case MyTimeline(n) =>
          {
            if (!tweetListByUser(n).isEmpty && PRINTON)
              print("User "+n+"'s Tweets: "+tweetListByUser(n)) 
          }

      }
  }
}