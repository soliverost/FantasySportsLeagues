# Fantasy Sports Leagues

## About the Project
This project is part of the Insight Data Science's Engineering fellowship program the I participated from January 2014 through March 2015.

Current process:

1. Engineer league information

2. Simple pipeline to transform the league information

	1. Run a spark job on HDFS and save the results to Cassandra.

	2. Run a simple Flask server to read from Cassandra.

	3. Run a Spark Streaming job that takes info from Kafka and outputs to Cassandra.