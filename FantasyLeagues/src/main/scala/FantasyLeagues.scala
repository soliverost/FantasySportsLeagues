import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object FantasyLeague {

	def main(args: Array[String]){

		val conf = new SparkConf(true)
					.set("spark.cassandra.connection.host", "54.183.167.48")
    				.setAppName("FantasyLeague")

    	val sc = new SparkContext(conf)

		val file = sc.textFile("hdfs://ip-172-31-13-111.us-west-1.compute.internal:8020/user/solivero/fantasyfootball/leagues/Leagues_10000.csv")
		val counts = file.flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey(_ + _)
		counts.saveToCassandra("fantasy", "players", SomeColumns("name", "count"))

	}
}



