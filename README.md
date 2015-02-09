# Fantasy Sports Leagues

### About
Fantasy Sports Leagues is my Data Engineering project as part of [Insight Data Science's Engineering](http://insightdataengineering.com) fellowship program 2015A. 

### Introduction
I decided to combine my love for data and sports during my project. 

While still focusing on the Data Engineering aspect, I thought it would be interesting to learn about the implications of trying to develop a pipeline that updates with real-time events and serves a user base of ~ 5 million people. 

My technology stack includes: Kafka, HDFS, Spark, Spark Streaming, and Cassandra and it is described below.

### Website
The project is currently hosted at [http://4fsports.net](http://4fsports.net)

### Setup
The pipeline lives on an AWS EC2 cluster. 

* Three instances are dedicated to Cloudera's Hadoop Distribution (CDH5, Cloudera Manager 5.1.4).
* Three instances are dedicated to Datastax AMI distribution of Cassandra.
* One micro-instance is dedicated to hosting the Flask Web Server. [Developed in a different repository](http://https://github.com/soliverost/FantasySportsLeaguesWebsite)

### Pipeline

Below is a general overview of the pipeline I'm using:

![alt text](https://github.com/soliverost/FantasySportsLeagues/img/pipeline.png)


#### Data Ingestion


### Future Plans
My grand vision is to implement real-time game substitutions. I would enjoy seeing if the users can substitute players in the middle of the games and still get the points. Besides the logistic aspect of allowing 

```javascript
$(function(){
  $('div').html('I am a div.');
});
```


