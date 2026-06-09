[![big-industries-logo](https://www.bigindustries.be/hubfs/popular-
bigindustries/big-industries-logo.svg)](//bigindustries.be)

  * [Home](https://www.bigindustries.be)
  * [Services](https://www.bigindustries.be/services)
  * [References](https://www.bigindustries.be/references)
  * [Team](https://www.bigindustries.be/our-team)
  * [Projects](https://www.bigindustries.be/projects)
  * [Jobs](https://www.bigindustries.be/jobs)
  * [Blogs](https://www.bigindustries.be/blog)

[ Contact ](https://www.bigindustries.be/contact)

######  [cloudera](https://www.bigindustries.be/blog/tag/cloudera)

# Creating a Data Pipeline using Flume, Kafka, Spark and Hive

[ Mouzzam ](https://www.bigindustries.be/blog/author/mouzzam)

Jun 29, 2016 7:52:39 PM

SHARE [
](https://www.facebook.com/sharer/sharer.php?u=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fcreating-
a-data-pipeline-using-flume-kafka-spark-and-hive) [
](https://twitter.com/intent/tweet?original_referer=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fcreating-
a-data-pipeline-using-flume-kafka-spark-and-
hive&url=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fcreating-a-data-pipeline-
using-flume-kafka-spark-and-
hive&source=tweetbutton&text=Creating+a+Data+Pipeline+using+Flume%2C+Kafka%2C+Spark+and+Hive)
[
](https://www.linkedin.com/shareArticle?mini=true&url=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fcreating-
a-data-pipeline-using-flume-kafka-spark-and-
hive&title=Creating+a+Data+Pipeline+using+Flume%2C+Kafka%2C+Spark+and+Hive&summary=The+aim+of+this+post+is+to+help+you+...)
[
](mailto:?subject=Check%20out%20Creating%20a%20Data%20Pipeline%20using%20Flume,%20Kafka,%20Spark%20and%20Hive%20&body=Check%20out%20https://www.bigindustries.be/blog/creating-
a-data-pipeline-using-flume-kafka-spark-and-hive)

_The aim of this post is to help you getting started with creating a data
pipeline using flume, kafka and spark streaming that will enable you to fetch
twitter data and analyze it in hive._

### The setup

We will use flume to fetch the tweets and enqueue them on kafka and flume to
dequeue the data hence flume will act both as a kafka producer and consumer
while kafka would be used as a channel to hold data. This approach is also
informally known as “flafka”. We will use the flume agent provided by cloudera
to fetch the tweets from the twitter api. This data would be stored on kafka
as a channel and consumed using flume agent with spark sink. Spark streaming
will read the polling stream from the custom sink created by flume. Spark
streaming app will parse the data as flume events separating the headers from
the tweets in json format. Once spark has parsed the flume events the data
would be stored on hdfs presumably a hive warehouse. We can then create an
external table in hive using hive SERDE to analyze this data in hive. The data
flow can be seen as follows:



### **Docker**

All of the services mentioned above will be running on docker instances also
known as docker container instances. We will run three docker instances, more
details on that later. If you don’t have docker available on your machine
please go through the Installation section otherwise just skip to launching
the required docker instances.

### **Installing Docker**

If you do not have docker, First of all you need to install docker on your
system. You will find detailed instructions on installing docker
at[https://docs.docker.com/engine/installation/](https://docs.docker.com/engine/installation/)

Once docker is installed properly you can verify it by running a command as
follows:

    
    
    $ docker run hello-world
    
    
    
    
    Unable to find image 'hello-world:latest' locally
    
    
    latest: Pulling from library/hello-world
    
    
    03f4658f8b78: Pull complete 
    
    
    a3ed95caeb02: Pull complete 
    
    
    Digest: sha256:8be990ef2aeb16dbcb9271ddfe2610fa6658d13f6dfb8bc72074cc1ca36966a7
    
    
    Status: Downloaded newer image for hello-world:latest
    
    
    Hello from Docker.
    
    
    This message shows that your installation appears to be working correctly.
    
    
    To generate this message, Docker took the following steps:

  1. The Docker client contacted the Docker daemon.

  2. The Docker daemon pulled the "hello-world" image from the Docker Hub.

  3. The Docker daemon created a new container from that image which runs the

    
    
        executable that produces the output you are currently reading.

  4. The Docker daemon streamed that output to the Docker client, which sent it

    
    
        to your terminal.
    
    
    To try something more ambitious, you can run an Ubuntu container with:
    
    
     $ docker run -it ubuntu bash
    
    
    Share images, automate workflows, and more with a free Docker Hub account:
    
    
     https://hub.docker.com
    
    
    For more examples and ideas, visit:
    
    
     https://docs.docker.com/userguide/
    
    

### **Launching the required docker container instances**

We will be launching three docker instances namely kafka, flume and spark.
Please note that the names Kafka, Spark and Flume are all separate docker
instances of  “cloudera/quickstart” – https://github.com/caioquirino/docker-
cloudera-quickstart. The Kafka container instance, as suggested by its name,
will be running an instance of the Kafka distributed message queue server
along with an instance of the Zookeeper service. We can use the instance of
this container to create a topic, start producers and start consumers – which
will be explained later. The Flume and Spark container instances will be used
to run our Flume agent and Spark streaming application respectively.

The following figure shows the running containers:

    
    
     

You can launch the docker instance for kafka as follows:

    
    
    $ docker run --hostname=quickstart.cloudera --name kafka --privileged=true -t -i -v $HOME/SparkApp/Flafka2Hive:/app -e KAFKA="localhost:9092" -e ZOOKEEPER="localhost:2181" cloudera/quickstart:latest /usr/bin/docker-quicks**tart**

Notice that we are running the container by giving it the name (–name) kafka
and providing two environment variables namely KAFKA and ZOOKEEPER. The name
of this container will be used later to link it with the flume container
instance.

*if this container image is not already present on your machine, docker will automatically download the instance and launch it. Make sure you have enough RAM to run the docker instances, as they can chew through quite a lot! Once this process is finished you can make sure that the kafka container is running as follows:
    
    
    $ docker ps
    
    
    #output of docker ps
    
    
    CONTAINER ID        IMAGE                        COMMAND                  CREATED             STATUS              PORTS                                              NAMES
    
    
    af371c24f8c1        cloudera/quickstart:latest   "/usr/bin/docker-quic"   46 hours ago       Up 46 hours                                                           kafka

Now let’s launch the flume and spark instances as follows:

First we will launch the cloudera docker instance for flume server as the
flume agent will run here, and then the spark instance.  (Please note that the
data required by each docker instance can be found at link here (*TODO* —
Directories and README.md already created just need to upload and provide the
download link*) The directory named FlumeData should be mounted to the flume
docker instance and the directory named SparkApp should be mounted to the
spark docker instance as shown by the following commands:

    
    
    $ docker run --hostname=flume --name flume --link kafka:kafka --privileged=true -t -i -v $HOME/FlumeData:/app cloudera/quickstart:latest /usr/bin/docker-quickstart

The docker instance named “flume” will be linked to the kafka instance as it
is pulling tweets enqueuing them on the kafka channel.

    
    
    $ docker run --hostname=quickstart.cloudera --name spark --link kafka:kafka --privileged=true -t -i -v $HOME/SparkApp/Flafka2Hive:/app cloudera/quickstart:latest /usr/bin/docker-quickstart

_* there is a bug in the Cloudera docker instance that if the hostname is set
to something other than “quickstart.cloudera” at the docker run command line,
then launching the spark app fails._

For this post, we will use the spark streaming-flume polling technique. The
spark instance is  linked to the “flume” instance and the flume agent dequeues
the flume events from kafka into a spark sink. This essentially creates a
custom sink on the given machine and port, and buffers the data until spark-
streaming is ready to process it.

Please note that we named the docker instance that would run flume agent as
flume, and mounted the relevant flume dependencies and the the flume agent
available in the directory **$HOME/FlumeData**  by using the -V parameter. The
–link parameter is linking the flume container to kafka container which is
very important as if you do not link the two container instances they are
unable to communicate.

Once the image has finished downloading, docker will launch and run the
instance, and you will be logged into the spark container as the container’s
root user:

    
    
    Started Impala Catalog Server (catalogd) :                 [  OK  ]
    
    
    Started Impala Server (impalad):                           [  OK  ]
    
    
    [root@quickstart /]#

You can exit the docker instance’s shell by typing exit, but for now we’ll
leave things as they are.

We can open a new terminal and use it to verify if both the container
instances are running as follows:

    
    
    $ docker ps
    
    
    CONTAINER ID        IMAGE                        COMMAND                  CREATED             STATUS              PORTS                                              NAMES
    
    
    af371c24f8c1        cloudera/quickstart:latest   "/usr/bin/docker-quic"   46 hours ago       Up 46 hours                                                           kafka
    
    
    abca402dc111        cloudera/quickstart:latest   "/usr/bin/docker-quic"   28 hours ago        Up 28 hours         0.0.0.0:32805->7180/tcp, 0.0.0.0:32804->8888/tcp   spark
    
    
    dd7e2fb5cc9a        cloudera/quickstart:latest   "/usr/bin/docker-quic"   46 hours ago        Up 46 hours         0.0.0.0:32771->7180/tcp, 0.0.0.0:32770->8888/tcp   flume

you can run the following commands to grab the container ids in a variable:

    
    
    KAFKA_SERVER=`docker ps | grep kafka | awk '{print $1}'`
    
    
    FLUME_SERVER=`docker ps | grep flume | awk '{print $1}'`
    
    
    SPARK_SERVER=`docker ps | grep spark | awk '{print $1}'`

### **Creating a topic in Kafka**

You have to create a topic in kafka so that your producers and consumers can
enqueue/dequeue data respectively from this topic. We are going to create the
topic named **twitter**. you should be logged into the kafka instance of
docker in order to create the topic. To login use the following command:

    
    
    $ docker exec -it $KAFKA_SERVER /bin/bash

Once in the kafka shell you are ready to create the topic:

    
    
    $ kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 1 --partitions 2 --topic twitter
    
    
    $ exit

### **Flume agent configuration**

Now we can put together the conf file for a flume agent to enqueue the tweets
in kafka on the topic named twitter that we created in the previous step.

    
    
     $ docker exec -it $FLUME_SERVER /bin/bash

Once you are in the flume instance’s shell, you can configure and launch the
flume agent called twitterAgent for fetching tweets. Before creating the flume
agent conf, we will copy the flume dependencies to the flume-ng lib directory
as follows:

    
    
    $ cp app/flume_dependencies/* /usr/lib/flume-ng/lib/

Now we will configure our flume agent. This agent is configured to use kafka
as the channel and spark streaming as the sink. you can create and launch the
flume instance as follows:

    
    
    $ flume-ng agent -Xmx512m -f app/twitter-kafka.conf -Dflume.root.logger=INFO,console -n twitterAgent
    
    
    $ cat conf/twitter-kafka.conf
    
    
    twitterAgent.sources.twitter-data.type = com.cloudera.flume.source.TwitterSource
    
    
    twitterAgent.sources.twitter-data.consumerKey = “Your consumerKey”
    
    
    twitterAgent.sources.twitter-data.consumerSecret = “Your consumerSecret”
    
    
    twitterAgent.sources.twitter-data.accessToken = “Your accessToken”
    
    
    twitterAgent.sources.twitter-data.accessTokenSecret = “Your accessTokenSecret”
    
    
    twitterAgent.sources.twitter-data.channels = kafka-channel
    
    
    twitterAgent.sources.twitter-data.keywords = cloudera, java, bigdata, mapreduce, mahout, hbase, nosql, hadoop, hive, spark, kafka, flume, scala
    
    
    twitterAgent.channels.kafka-channel.type = org.apache.flume.channel.kafka.KafkaChannel
    
    
    twitterAgent.channels.kafka-channel.capacity = 10000
    
    
    twitterAgent.channels.kafka-channel.transactionCapacity = 1000
    
    
    twitterAgent.channels.kafka-channel.brokerList = kafka:9092
    
    
    twitterAgent.channels.kafka-channel.topic = twitter
    
    
    twitterAgent.channels.kafka-channel.zookeeperConnect = kafka:2181
    
    
    twitterAgent.channels.kafka-channel.parseAsFlumeEvent = true
    
    
    twitterAgent.sinks = spark
    
    
    twitterAgent.sinks.spark.type = org.apache.spark.streaming.flume.sink.SparkSink
    
    
    twitterAgent.sinks.spark.hostname = <chosen machine's hostname>  for example : 192.168.1.1
    
    
    twitterAgent.sinks.spark.port = <chosen machine's port> for example : 8677
    
    
    twitterAgent.sinks.spark.channel = kafka-channel

When you launch your flume agent you will see lots of output in the terminal:

When you see the INFO on console connected to kafka for producing, this means
that the flume agent has started receiving data which is sent to the kafka
topic named twitter. To verify that kafka is receiving the messages we can run
a kafka consumer to verify that there is data on the channel, jump on the
kafka shell and create a consumer as follows:

    
    
    $ KAFKA_SERVER=`docker ps | grep kafka | awk '{print $1}'`
    
    
    $ docker exec -it $KAFKA_SERVER /bin/bash
    
    
    kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic twitter

If everything is configured well you should be able to see tweets in JSON
formatting as flume events with a header.  

### **Spark Streaming**

The flume agent should also be running a spark sink. As previously noted, the
spark sink that we configured for the flume agent is using the poll based
approach. It creates a custom flume sink and instead of sending the data
directly to spark streaming the custom sink buffers the data until spark
streaming receives and replicates the data. More details on the flume poll
based approach, and other options, can be found  in the spark documentation at
http://spark.apache.org/docs/latest/streaming-flume-integration.html.

We will use sbt  for dependency management, and IntelliJ as the IDE.

build.sbt looks as follows:

    
    
    _name_ := "Flafka2Hive"
    
    
    _scalaVersion_ := "2.10.5"
    
    
    **val** sparkVersion = "1.6.1"
    
    
    **val** avroVersion = "2.0.1"
    
    
    **val** bijectionVersion = "0.7.1"
    
    
    _libraryDependencies_ ++= _Seq_(
    
    
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    
    
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    
    
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    
    
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    
    
    "org.apache.spark" % "spark-streaming-flume_2.10" % %sparkVersion,
    
    
    "com.databricks" %% "spark-avro" % avroVersion,
    
    
       ("org.apache.spark" %% "spark-streaming-kafka" % sparkVersion) exclude ("org.spark-project.spark", "unused")
    
    
     
    
    
    )
    
    
    _assemblyJarName_ in _assembly_ := _name_.value + ".jar"

  
Now lets see some spark code:

    
    
    **package** be.bigindustries.spark
    
    
    **import** org.apache.spark.streaming._
    
    
    **import** org.apache.spark.streaming.flume.FlumeUtils
    
    
    **import** org.apache.spark.{SparkConf, SparkContext}
    
    
    **object** FlafkaTweet2Hive {
    
    
    **def** main(args: Array[String]) {
    
    
       **if**(args.length < 3) {
    
    
         System._err_.println(s"""
    
    
                               |Usage: KafkaTweet2Hive <hostname> <port>
    
    
                               |  <hostname> flume spark sink hostname
    
    
                               |  <port> port for spark-streaming to connect
    
    
                               |  <filePath> the path on HDFS where you want to write the file containing tweets
    
    
           """.stripMargin)
    
    
         System._exit_(1)
    
    
       }
    
    
       //Matching the arguments
    
    
       **val** _Array_(hostname, port, path) = args
    
    
       // Create context with 2 second batch interval
    
    
       **val** sparkConf = **new** SparkConf().setAppName("KafkaTweet2Hive")
    
    
       **val** sc = **new** SparkContext(sparkConf)
    
    
       **val** ssc = **new** StreamingContext(sc, _Seconds_(5))
    
    
       //Create a PollingStream
    
    
       **val** stream = FlumeUtils._createPollingStream_(ssc, hostname, port.toInt)
    
    
       //Parse the flume events to get tweets
    
    
       **val** tweets = stream.map(e => **new** String(e._event_.getBody.array))
    
    
       //Write the parsed tweets to a file
    
    
       **if**(tweets.count().!=(0)){
    
    
         tweets.saveAsTextFiles(path)
    
    
       }
    
    
       // Start the computation
    
    
       ssc.start()
    
    
       ssc.awaitTermination()
    
    
     }
    
    
    }

You have to generate the Jar file which can be done using sbt or in intelliJ.

Once the JAR file is generated you are ready to deploy it to the spark docker
instance via the shared directory that we set up and run it as mentioned in
the launching docker instances section.

### **Launch Spark Application**

    
    
    $ spark-submit --master yarn-client --driver-memory 1g --executor-memory 1g --executor-cores 1 --class be.bigindustries.spark.FlafkaTweet2Hive /app/flafka2hive_2.10-0.1-SNAPSHOT.jar  <hostname> <port> <path-to-save-output-file> 

An example of the runtime arguments for this class is as follows:

    
    
    172.17.0.3 8677 /user/hive/warehhouse/tweets

You can verify if spark streaming is populating the data as follows:

    
    
    $ hdfs dfs -ls /user/hive/warehouse/tweets

Once you have seen the files, you can start analysis on the data using hive as
shown in the following section.

### **Analyzing the data in Hive**

The Spark streaming consumer app has parsed the flume events and put the data
on hdfs. You can now read the data using a hive external table for further
processing.

login to the Flume/spark server as follows:

    
    
    $ docker exec -it $SPARK_SERVER /bin/bash

Start the hive shell as follows:

    
    
    $ hive

We should now be in the hive shell as follows:

    
    
    hive>

First let’s add the serde jar for JSON so Hive can understand the data format:

    
    
    hive> ADD JAR app/hive-serdes-1.0-SNAPSHOT.jar;

Now let’s create an external table in Hive so we can query the data:

    
    
    CREATE EXTERNAL TABLE tweets (
    
    
    **  **id BIGINT,
    
    
    **  **created_at STRING,
    
    
      source STRING,
    
    
      favorited BOOLEAN,
    
    
      retweeted_status STRUCT<
    
    
        text:STRING,
    
    
        user:STRUCT<screen_name:STRING,name:STRING>,
    
    
        retweet_count:INT>,
    
    
      entities STRUCT<
    
    
        urls:ARRAY<STRUCT<expanded_url:STRING>>,
    
    
        user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
    
    
        hashtags:ARRAY<STRUCT<text:STRING>>>,
    
    
      text STRING,
    
    
    **  **user STRUCT<
    
    
        screen_name:STRING,
    
    
        name:STRING,
    
    
        friends_count:INT,
    
    
        followers_count:INT,
    
    
        statuses_count:INT,
    
    
        verified:BOOLEAN,
    
    
        utc_offset:INT,
    
    
        time_zone:STRING>,
    
    
      in_reply_to_screen_name STRING
    
    
    )
    
    
    ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
    
    
    LOCATION '/user/hive/warehouse/tweets';

Verify if the data is populated in the table as follows:

    
    
    hive> select count(*) from tweets;

You should be able to see a non zero entry.

Let’s see the top 20 hashtags based on the user location by running the
following query:

    
    
    hive> SELECT LOWER(hashtags.text) AS HASHTAG, user.time_zone, count(*) as CountPerZone FROM tweets WHERE user.time_zone IS NOT NULL LATERAL VIEW EXPLODE(entities.hashtags) t1 AS hashtags GROUP BY LOWER(hashtags.text), user.time_zone Order By CountPerZone DESC Limit 20;

The result of the query should be as follows:



### **Stopping Docker Containers**

Once you are done, it is important to stop and remove the docker containers
otherwise it can eat up system resources, especially disk space, very quickly.

    
    
    docker stop $FLUME_SERVER && docker rm $FLUME_SERVER
    
    
    docker stop $SPARK_SERVER && docker rm $FLUME_SERVER
    
    
    docker stop $KAFKA_SERVER && docker rm $KAFKA_SERVER

Verify that the docker instances are no longer present as follows:

    
    
    $ docker ps -a

if you have made the mistake of starting a few containers without removing
them while you stopped or kill them please check this discussion to see
options for freeing up some disk space by removing the stopped or old
instances:

[http://stackoverflow.com/questions/17236796/how-to-remove-old-docker-
containers](http://stackoverflow.com/questions/17236796/how-to-remove-old-
docker-containers)

#### [ Mouzzam ](https://www.bigindustries.be/blog/author/mouzzam)

Mouzzam is an IT professional with hands on knowledge of Data Engineering. He
has a keen interest in developing solutions for Big Data. He has gained
working experience in processing and analytics of massive datasets. He has
worked on Social Network analytics, graph databases and CDRs (Call Detail
Records). He is currently doing research in creating solutions for Getting
fast query results on Big Data using compact data representations.

## Related posts

At Big Industries, we want to share our wisdom. More info on our recent
developments, newest projects and upcoming events

[ ![Untitled](https://www.bigindustries.be/hubfs/spark-logo-trademark.png)
](https://www.bigindustries.be/blog/big-data-processing-with-apache-spark)

######  [Big Industries Academy](https://www.bigindustries.be/blog/tag/big-
industries-academy)

#### [Big Data processing with Apache
Spark](https://www.bigindustries.be/blog/big-data-processing-with-apache-
spark)

Apache Spark is an open source big data processing framework built around
speed, ease of use, and...

[ Read more ](https://www.bigindustries.be/blog/big-data-processing-with-
apache-spark)

[ ![Understanding Trino and How It Powers Starburst
Data](https://www.bigindustries.be/hubfs/trino-og.png)
](https://www.bigindustries.be/blog/understanding-trino-and-how-it-powers-
starburst-data)

######  [General](https://www.bigindustries.be/blog/tag/general)

#### [Understanding Trino and How It Powers Starburst
Data](https://www.bigindustries.be/blog/understanding-trino-and-how-it-powers-
starburst-data)

As organizations continue to adopt modern, decentralized data architectures,
the need for...

[ Read more ](https://www.bigindustries.be/blog/understanding-trino-and-how-
it-powers-starburst-data)

#### Subscribe to our newsletter

Stay informed about our recent developments, newest projects and upcoming
events

###

![img-
mails](https://f.hubspotusercontent10.net/hubfs/369261/raw_assets/public/Marketplace/leadstreet/themes/popular-
theme/images/img-mails.svg)

## Ready to set off on a BIG journey?

##### The top notch technologies we use set us apart from other consultancies  
  

[HAVE A LOOK AT OUR SERVICES](https://www.bigindustries.be/services)

**Big Industries NV**

**YOU THINK BIG, YOU GET BIG**

##  

[ ](https://www.facebook.com/Big-Industries-933906943382629/) [
](https://www.linkedin.com/company/4838922/)

**Contact us**

Veldkant 33a,  
2550 Kontich, Belgium  
[GET DIRECTIONS ](//www.bigindustries.be/contact)  
  
T +32 (0)3 450 80 30  
E [info@bigindustries.be](mailto:info@bigindustries.be)

**Hours**

Monday to Friday  
9am – 5pm CET  
  
Weekends  
Closed

##### **Drop us a line**

© Big Industries 2021 - all rights reserved

  * [TERMS AND CONDITIONS](https://www.bigindustries.be/terms-and-conditions)
  * [PRIVACY POLICY](https://www.bigindustries.be/privacy-policy)

