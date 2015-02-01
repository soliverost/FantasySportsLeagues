import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object TopUsers {

	def main(args: Array[String]){

		val conf = new SparkConf(true)
					.set("spark.cassandra.connection.host", "54.67.124.220")
    				.setAppName("TopUsers")

    	val sc = new SparkContext(conf)

		val file = sc.textFile("hdfs://ip-172-31-13-111.us-west-1.compute.internal:8020/user/solivero/fantasyfootball/userpoints/userPoints.csv")
		
		val topUsers = file.map({line => val pieces = line.split(",") 
			(pieces(0), pieces(1).toDouble)}).reduceByKey(_+_)

		// Swap the keys and values for sorting!
		val vk = topUsers.map(_.swap)
		val vkSorted = vk.sortByKey(false) //false=descending
		val topTen = sc.parallelize(vkSorted.take(10))

		topTen.saveToCassandra("fantasyfootball", "topusers", SomeColumns("points", "userid"))

	}
}
