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

import java.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


object playstreaming {

    def updateFunction(newValues: Seq[Double], runningCount: Option[Double]): Option[Double] = {

            val newCount = newValues.sum;  
            val oldCount = runningCount.getOrElse(0.0)

            Some((newCount + oldCount))
    }

  def main(args: Array[String]) {

    val zkQuorum = "ip-172-31-13-111.us-west-1.compute.internal:2181"
    val group = "rt"
    val topics = Map("playtest2" -> 1)
    val numThreads = 1

    val conf = new SparkConf()
                    .setAppName("playstreaming")
                    .set("spark.cassandra.connection.host", "54.67.124.220")
    val sc = new SparkContext(conf)

    // Set up my writer that writes to a single file
    val hadoopconf = new Configuration()
    val fs= FileSystem.get(hadoopconf)
    val path = new Path("/user/solivero/fantasyfootball/userpoints/userpoints_01.csv")
    val output = if(fs.exists(path)) fs.append(path) else fs.create(path)
    val writer = new PrintWriter(output)

    // 1, 1, Bruce Gradkowski, QB, 2014-08-02 10:00:00 -0700
    val file = sc.textFile("hdfs://ip-172-31-13-111.us-west-1.compute.internal:8020/user/solivero/fantasyfootball/playerselection/Leagues_Generated_0_1.csv")
    val playeruser = file.map({line => val pieces = line.split(",")
        (pieces(2),pieces(1).toInt)})

    val ssc =  new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("checkpoint")

    // Kafka consumer
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topics)

    // Map my kafka stream to the correct key,value:
    val testInput = lines.map({line => val pieces = line._2.split(",")
        (pieces(2),(pieces(3),pieces(0)))})
    testInput.print()
   
    // Join with the appropiate user and flatten
    val joinedUserPoints = testInput.transform(rdd=> rdd.join(playeruser))
    val userPointsFlat = joinedUserPoints.map{case(pname,((points,date),uid)) => (uid,date,points)}
    //userPointsFlat.print()
    //userPointsFlat.repartition(1).saveAsTextFiles("/user/solivero/fantasyfootball/userpoints_test/dom","csv")

    // Keep track of the score of the day
    val userPointsDay = userPointsFlat.map{case(uid,date,points) => (uid,points.toDouble)}
    val runningCounts = userPointsDay.updateStateByKey[Double](updateFunction _)
    //val runningCountsFlat = runningCounts.map{case((uid,points) => (uid,date,points)}
    //runningCounts.print()
    runningCounts.saveToCassandra("fantasyfootball","userpointsstream",SomeColumns("userid","points"))

    // Loop through and save in a single file to avoid lots of small files
    
    
    userPointsFlat.foreachRDD(rdd => {
        for(item <- rdd.collect().toArray) {
            try {
                writer.append("" + item._1)
                writer.append("," + item._2)
                writer.append("," + item._3)
                writer.append('\n') 
            }
        }
    })

    



    ssc.start()
    ssc.awaitTermination()

    // close the writer
    writer.close()

  }
}

