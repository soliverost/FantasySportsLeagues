import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object BatchQueries {

	def main(args: Array[String]){

		/** Set up configuration and context **/
		val conf = new SparkConf(true)
					.set("spark.cassandra.connection.host", "54.67.124.220")
    				.setAppName("BatchQueries")

    	val sc = new SparkContext(conf)

    	// To Do: encapsulate data better:
    	//case class User(id: Int, name: String)
		//case class UserGender(userId: Int, gender: String)

    	/** Read in the files that need to be processed 
    	 ** Player Data: date,time,player name, position, points
    	 ** User Points: userid, points, player name, date
    	 **/
		val userPoints = sc.textFile("hdfs://ip-172-31-13-111.us-west-1.compute.internal:8020/user/solivero/fantasyfootball/userpoints/userPoints.csv")
		val playerPoints = sc.textFile("hdfs://ip-172-31-13-111.us-west-1.compute.internal:8020/user/solivero/fantasyfootball/playerpoints/*")

		/** Calculate Top 10 Users for all time **/
		val topUsers = userPoints.map({line => val pieces = line.split(",") 
			(pieces(0), pieces(1).toDouble)}).reduceByKey(_+_)

		// Swap the keys and values for sorting!
		val vk = topUsers.map(_.swap).sortByKey(false) //false=descending
		val topTen = sc.parallelize(vk.take(10))

		//topTen.saveToCassandra("fantasyfootball", "topusers", SomeColumns("points", "userid"))

		/** Calculate User's points by different granularity: Month, Day, Hour **/

		/*
		val scoresMonth = file.map({line => val pieces = line.split(",")
			val datePieces = pieces(3).split("/")
			(pieces(0) + "|" + datePieces(0), pieces(1).toDouble)}).reduceByKey(_+_)
		*/

		val scoresmonth = userPoints.map({line => val pieces = line.split(",")
			val datePieces = pieces(3).split("/")
			((pieces(0),datePieces(0)),pieces(1).toDouble)}).reduceByKey(_+_)

		//scoresmonth.saveToCassandra("fantasyfootball","userpoints",SomeColumns("userid","date","points"))

		// Save as: userid, month, points


		val scoresday = userPoints.map({line => val pieces = line.split(",")
			val datePieces = pieces(3).split("/")
			((pieces(0),datePieces(1)),pieces(1).toDouble)}).reduceByKey(_+_)

		// Flatten the key to store in different columns
		// TODO: Check how I'm storing my userid... no real need for a double... date is also a string atm
		val scoresdayf = scoresday.map{case(composite,points) => (composite._1.toDouble,composite._2,points)}
		//scoresdayf.saveToCassandra("fantasyfootball","userpoints",SomeColumns("userid","date","points"))

		// Save as: userid, day, points

		/** Calculate Player's points by Game(~Day) **/

		val playerscore = playerPoints.map({line => val tokens = line.split(",")
			((tokens(0),tokens(2)),tokens(4).toDouble)}).reduceByKey(_+_)
		val playerscore_flat = playerscore.map{case(composite,points) => (composite._1,composite._2,points)}
		playerscore_flat.saveToCassandra("fantasyfootball","playerpoints",SomeColumns("date","playername","points"))

	}
}
