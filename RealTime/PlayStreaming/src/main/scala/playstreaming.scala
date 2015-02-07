import java.util.Properties

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._


object playstreaming {

    def updateFunction(newValues: Seq[Double], runningCount: Option[Double]): Option[Double] = {

        if(newValues.isEmpty){
            None
        } else{
            val newCount = newValues.sum;  
            val oldCount = runningCount.getOrElse(0.0)

            Some((newCount + oldCount))
        }
    }

  def main(args: Array[String]) {

    val zkQuorum = "localhost:2181"
    val group = "rt"
    val topics = Map("playplay2" -> 1)
    val numThreads = 1

    val conf = new SparkConf()
                    .setAppName("playstreaming")
                    .set("spark.cassandra.connection.host", "54.67.124.220")
    val sc = new SparkContext(conf)

    val ssc =  new StreamingContext(sc, Seconds(7))
    ssc.checkpoint("checkpoint")

    // 1, 1, Bruce Gradkowski, QB, 2014-08-02 10:00:00 -0700
    val file = sc.textFile("hdfs://ip-172-31-13-111.us-west-1.compute.internal:8020/user/solivero/fantasyfootball/playerselection/newOutput.csv")
    val playeruser = file.map({line => val pieces = line.split(",")
        (pieces(2),pieces(1).toInt)})

    // Kafka consumer
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topics)

    // Map my kafka stream to the correct key,value:
    val testInput = lines.map({line => val pieces = line._2.split(",")
        (pieces(2),(pieces(3),pieces(0)))})
    // testInput.print()
   
    // Join with the appropiate user and flatten
    val joinedUserPoints = testInput.transform(rdd=> rdd.join(playeruser))
    val userPointsFlat = joinedUserPoints.map{case(a,((b,c),d)) => (a,b,c,d)}
    //userPointsFlat.print()


    // Keep track of the score of the day
    val userPointsDay = userPointsFlat.map{case(player,points,date,uid) => ((uid,date),points.toDouble)}
    val runningCounts = userPointsDay.updateStateByKey[Double](updateFunction _)
    runningCounts.print()




    // New RDD from Cassandra Query
    // val playeruser = ssc.cassandraTable("playerfollowers", "playeruser").select("playername","userid")

    /*
    val stream_window_element = lines.countByWindow(Seconds(5), Seconds(1))
    stream_window_element.print()*/



    ssc.start()
    ssc.awaitTermination()
  }
}

