import akka.actor.{ActorSystem, ActorLogging, Actor, Props, ActorRef}
import java.security.MessageDigest
import com.typesafe.config.ConfigFactory
import scala.util.matching.Regex
import scala.collection.mutable.ArrayBuffer
import scala.math
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.remote.RemoteActorRefProvider
import scala.util.Random
import java.io._
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import akka.routing.SmallestMailboxRouter

object twitter {
  sealed trait TraitMessage
  case object Start extends TraitMessage
  case object Stop extends TraitMessage
  case object Terminated extends TraitMessage
  case object Hello extends TraitMessage
  case class TweetReceive(from : Int, tweetString : String) extends TraitMessage
  case class TweetFollowers(from : Int, tweetString : String, to : Int) extends TraitMessage
  case class Calculate(system : ActorSystem) extends TraitMessage
  case class StartTweeting(system : ActorSystem) extends TraitMessage
  case class WriteTweet(from : Int, tweet : String) extends TraitMessage
  case class Ping(str: String) extends TraitMessage
  var worker = ArrayBuffer[ActorRef]()
  var tweets = ListBuffer(Queue[String]())
  var usersTable = new ArrayBuffer[ArrayBuffer[Int]]()
  var cores = Runtime.getRuntime().availableProcessors();
  var countFollow = 0
  val WRITEON = false
  val SAVETODISK = false
  val TIMEOUT = 10000
  var currentNumWorkers = 0
  class Worker(numOfUsers : Int) extends Actor with ActorLogging {
    val numUsers = numOfUsers
    var tweetcountperuser = 0
    def receive = {
      case Ping(str) ⇒ {
        val selection = context.actorSelection("akka.tcp://TwitterSystem@"+str+":5150/user/Server")
        selection ! Hello
      }

      case Start => {
        var str = ""
        str = Random.nextString(Random.nextInt(140))
        tweetcountperuser = tweetcountperuser +1
        //println(tweetcountperuser + " " + self)
        sender ! TweetReceive(Random.nextInt(numUsers),str)
      }
      case Stop => {
        sender ! Terminated
        context.stop(self)
      }
      case TweetFollowers(from,tweetString,to) => {
        countFollow = countFollow + 1
      }
    }
  }

  class TweetWriter extends Actor with ActorLogging {
    def receive = {

      case WriteTweet(from, tweet)=> {
        var filename = "tweets/"+from.toString+".txt"
        var outfile = new File(filename)
        var dir = new File("tweets")
        if(!dir.exists)
          dir.mkdir()
        if(!outfile.exists)
        {
          var writer = new PrintWriter(filename, "UTF-8");
          writer.println(tweet)
          writer.close()
        }
        else
        {
          var writer = new FileWriter(filename, true);
          writer.write(tweet)
          writer.close()
        }
      }
    }
  }

  class Master(numOfUsers : Int,numOfActors : Int) extends Actor {
    var numUsers = numOfUsers
    var numActors = numOfActors
    var tWriter = context.actorOf(Props(new TweetWriter() ), name = "writer")
    var countTweet = 0
    var numterminated = 0
    var sum = 0
    var b = System.currentTimeMillis()
    def receive = {
      case Calculate(system) ⇒ {
        for (i <- 0 to numUsers - 1 ) {
          usersTable.+=(ArrayBuffer[Int]())
          tweets.+=(new Queue[String])
          var usernum = Random.nextInt(numUsers)
          sum = sum + usernum
          var rand = 0
          for (j <- 0 to usernum - 1) {
            rand = Random.nextInt(numUsers)
            while(usersTable(i).contains(rand) || rand == i)
              rand = Random.nextInt(numUsers)
            usersTable(i).+=(rand)
          }
        }
        println("Average is " + sum/numUsers)
        for (i <- 0 to numOfActors - 1 )
          worker += context.actorOf(Props(new Worker(numUsers) ), name = "worker"+i)
        currentNumWorkers = numOfActors
        //val server_actors = context.actorOf(Props[Master].withRouter(SmallestMailboxRouter(((cores/8).ceil).toInt)))
        self ! StartTweeting(system)
        b = System.currentTimeMillis()
      }

      case StartTweeting(system) => {
        import system.dispatcher
        for (i <- 0 to numActors - 1 )
          system.scheduler.schedule(0 milliseconds,10 milliseconds,worker(i),Start)
      }

      case TweetReceive(from,tweetString) => {
        countTweet = countTweet + 1
        if(WRITEON)
           tWriter ! WriteTweet(from,tweetString)
        else {
          tweets(from).+=(tweetString)
          if(tweets(from).size > 100) {
            var poptweet = tweets(from).dequeue()
            if(SAVETODISK)
              tWriter ! WriteTweet(from, poptweet)
          }
        }
        for (i <- 0 to usersTable(from).length - 1 ) {
          var rand = Random.nextInt(numActors)
          if(!worker(rand).isTerminated)
            worker(rand) ! TweetFollowers(from,tweetString,usersTable(from)(i))
          else
            println("dead")
        }
        ;println(countTweet + " " + countFollow)
      }

      case Terminated => {
        numterminated = numterminated + 1
        if(numterminated == numActors)
          context.system.shutdown()
      }

      case "KillSystem" => {
        if(System.currentTimeMillis() - b > TIMEOUT)
          System.exit(0)
      }
      case Hello => {
        println(sender+" connected")
      }
    }
  }

  def main(args: Array[String]) {
    if(args(1) forall Character.isDigit) {
      val configM = ConfigFactory.parseString("""akka
      {
        actor
        {
          provider = "akka.remote.RemoteActorRefProvider"
        }
        remote
        {
          enabled-transports = ["akka.remote.netty.tcp"]
          netty.tcp
          {
            hostname = "127.0.0.1"
            port = 5150
          }
        }     
      }""")

      val system = ActorSystem("TwitterSystem", ConfigFactory.load(configM))
      //val system = ActorSystem("TwitterSystem")
      val master = system.actorOf(Props(new Master(args(0).toInt,args(1).toInt) ), name = "Server")
      master ! Calculate(system)
      import system.dispatcher
      system.scheduler.schedule(0 milliseconds,1000 milliseconds,master,"KillSystem")
    }
    else {
      val configC = ConfigFactory.parseString("""akka
      {
        actor
          {
            provider = "akka.remote.RemoteActorRefProvider"
          }
          remote
          {
            enabled-transports = ["akka.remote.netty.tcp"]
            netty.tcp
            {
              hostname = "127.0.0.1"
              port = 0
            }
          }     
      }""")
    
      val app2 = ActorSystem("MyApp2", ConfigFactory.load(configC))
      for (i <- 0 to args(0).toInt - 1 ) {
        currentNumWorkers = currentNumWorkers + 1
        val worker = app2.actorOf(Props(new Worker(args(2).toInt)), name = "worker" + currentNumWorkers)
        worker ! Ping(args(1))
      }
    }
  }
}