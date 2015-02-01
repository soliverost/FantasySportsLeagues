import java.util.Properties

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object playstreaming {
  def main(args: Array[String]) {

    val zkQuorum = "localhost:2181"
    val group = "rt"
    val topics = Map("playplay" -> 1)
    val numThreads = 1

    // val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf()
                    .setAppName("playstreaming")
                    .set("spark.cassandra.connection.host", "54.67.124.220")
    val ssc =  new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topics)

    // Splitting my lines to get the name of the player
    lines.foreachRDD(rdd => {

        for(item <- rdd.collect().toArray) {
            val separated = item._2.split(",")
        }

        val rdd = ssc.cassandraTable("playerfollowers", "playeruser").select("userid").where("playername = ?", " " + separated(2)).toArray.foreach(println)
        
    })


    // Ask Cassandra for the users
   


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

