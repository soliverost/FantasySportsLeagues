import java.util.Properties

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object playstreaming {
  def main(args: Array[String]) {

    val zkQuorum = "localhost:2181"
    val group = "rt"
    val topics = Map("playplay" -> 1)
    val numThreads = 1

    val conf = new SparkConf()
                    .setAppName("playstreaming")
                    .set("spark.cassandra.connection.host", "54.67.124.220")
    val ssc =  new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoint")

    // We read in:
    //** Player Data: date,time,player name, points
    //** User Points: userid, points, player name, date 

    // New RDD from Cassandra Query
    val playeruser = ssc.cassandraTable("playerfollowers", "playeruser").select("playername","userid")

    // Kafka consumer
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topics)

    val testInput = lines.map({line => val pieces = line._2.split(",")
        (pieces(2),(pieces(3),pieces(0)))})

    testInput.print()

    // Join the players with the respective new points
    val joinedUserPoints = testInput.transform(rdd => rdd.join(playeruser))
    joinedUserPoints.first().print()



    /**

        // Batch query:
        val topUsers = userPoints.map({line => val pieces = line.split(",") 
            (pieces(0), pieces(1).toDouble)}).reduceByKey(_+_)

        // Swap the keys and values for sorting!
        val vk = topUsers.map(_.swap).sortByKey(false) //false=descending
        val topTen = sc.parallelize(vk.take(10))

        topTen.saveToCassandra("fantasyfootball", "topusers", SomeColumns("points", "userid"))
    **/



    // Splitting my lines to get the name of the player
    /**
    lines.foreachRDD(rdd => {

        for(item <- rdd.collect().toArray) {
            val separated = item._2.split(",")
        }

        val rdd = ssc.cassandraTable("playerfollowers", "playeruser").select("userid").where("playername = ?", " " + separated(2)).toArray.foreach(println)
        
    })**/


    // Ask Cassandra for the users
    // Read from HDFS

    //lines

    /*
    val stream_element = lines.count()
    stream_element.print()

    val stream_window_element = lines.countByWindow(Seconds(5), Seconds(1))
    stream_window_element.print()*/



    ssc.start()
    ssc.awaitTermination()
  }
}

