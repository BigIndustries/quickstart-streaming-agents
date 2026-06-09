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

######  [Big Industries Academy](https://www.bigindustries.be/blog/tag/big-
industries-academy)

# Apache Iceberg: A Table Format for Large Scale Data

![Matthias Vallaey](https://www.bigindustries.be/hs-fs/hubfs/popular-
bigindustries/team/Matthias-Vallaey.png?width=56&name=Matthias-Vallaey.png)

[ Matthias Vallaey ](https://www.bigindustries.be/blog/author/matthias-
vallaey)

Dec 27, 2023 2:44:40 PM

SHARE [
](https://www.facebook.com/sharer/sharer.php?u=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fapache-
iceberg-a-table-format-for-large-scale-data) [
](https://twitter.com/intent/tweet?original_referer=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fapache-
iceberg-a-table-format-for-large-scale-
data&url=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fapache-iceberg-a-table-
format-for-large-scale-
data&source=tweetbutton&text=Apache+Iceberg%3A+A+Table+Format+for+Large+Scale+Data)
[
](https://www.linkedin.com/shareArticle?mini=true&url=https%3A%2F%2Fwww.bigindustries.be%2Fblog%2Fapache-
iceberg-a-table-format-for-large-scale-
data&title=Apache+Iceberg%3A+A+Table+Format+for+Large+Scale+Data&summary=A+comparison+with+Apache+Kudu+and+Delta+...)
[
](mailto:?subject=Check%20out%20Apache%20Iceberg:%20A%20Table%20Format%20for%20Large%20Scale%20Data%20&body=Check%20out%20https://www.bigindustries.be/blog/apache-
iceberg-a-table-format-for-large-scale-data)



![Iceberg-2](https://www.bigindustries.be/hs-
fs/hubfs/Iceberg-2.png?width=690&height=473&name=Iceberg-2.png)

##### A comparison with Apache Kudu and Delta Lake

Apache Iceberg is an open source table format for storing and querying large-
scale data sets. It is designed to improve the performance, reliability and
scalability of data lake analytics. Iceberg supports both batch and streaming
data sources, and provides a rich set of features such as schema evolution,
partitioning, time travel, snapshots, transactions and row-level deletes.
Iceberg also integrates with popular query engines such as Apache Spark,
Apache Flink, Apache Hive and Presto.

#### How does Iceberg partition data?

Iceberg supports two types of partitioning: identity partitioning and bucket
partitioning. Identity partitioning assigns each data file to a partition
based on the value of one or more columns. For example, if a table is
partitioned by date, each data file will belong to a specific date partition.
Bucket partitioning assigns each data file to a partition based on a hash
function of one or more columns. For example, if a table is bucketed by
user_id, each data file will belong to a specific user_id bucket. Bucket
partitioning can help reduce data skew and improve join performance.

#### How does Iceberg support time travel and snapshots?

Iceberg supports time travel and snapshots by maintaining a history of table
metadata. Each change to the table, such as adding, deleting or updating data
files, creates a new snapshot of the table metadata. Each snapshot has a
unique ID and a timestamp, and can be referenced by queries. Iceberg also
keeps track of the parent-child relationship between snapshots, forming a
snapshot lineage. This allows users to query the table at any point in time,
or to roll back the table to a previous state.

#### How does Iceberg compare with Apache Kudu?

Apache Kudu is another open source table format for storing and querying
large-scale data sets. Kudu is optimized for fast analytics on fast data, such
as real-time or near-real-time data. Kudu supports both row-oriented and
column-oriented storage, and provides features such as schema evolution,
partitioning, compression, encryption and row-level updates. Kudu also
integrates with popular query engines such as Apache Spark, Apache Impala and
Presto.

Some of the differences between Iceberg and Kudu are:

  * Iceberg supports both batch and streaming data sources, while Kudu is mainly focused on streaming data sources.
  * Iceberg supports bucket partitioning, while Kudu only supports range and hash partitioning.
  * Iceberg supports time travel and snapshots, while Kudu does not.
  * Iceberg supports transactions and row-level deletes, while Kudu only supports row-level updates.
  * Iceberg uses a file-based storage layer, such as HDFS or S3, while Kudu uses its own storage layer, which requires dedicated servers and disks.

#### How does Iceberg compare with Delta Lake?

Delta Lake is another open source table format for storing and querying large-
scale data sets. Delta Lake is developed by Databricks, and is based on the
Spark SQL engine. Delta Lake supports both batch and streaming data sources,
and provides features such as schema evolution, partitioning, time travel,
snapshots, transactions and row-level updates and deletes. Delta Lake also
integrates with popular query engines such as Apache Spark, Apache Hive and
Presto.

Some of the similarities and differences between Iceberg and Delta Lake are:

  * Both Iceberg and Delta Lake support batch and streaming data sources, schema evolution, partitioning, time travel, snapshots, transactions and row-level updates and deletes.
  * Both Iceberg and Delta Lake use a file-based storage layer, such as HDFS or S3, and store table metadata in JSON files.
  * Iceberg supports bucket partitioning, while Delta Lake only supports range and hash partitioning.
  * Iceberg supports identity partitioning, while Delta Lake does not.
  * Iceberg supports multiple query engines, such as Spark, Flink, Hive and Presto, while Delta Lake is mainly based on Spark SQL.
  * Iceberg is designed to be independent of any specific query engine, while Delta Lake is tightly coupled with Spark SQL.

[![Need help with your Data Lakehouse Project?](https://no-
cache.hubspot.com/cta/default/2240994/5e3dca1d-edfb-467c-8ae0-d706a3083419.png)](https://cta-
redirect.hubspot.com/cta/redirect/2240994/5e3dca1d-edfb-467c-8ae0-d706a3083419)

source image: Ryan Blue - Tabular



![Matthias Vallaey](https://www.bigindustries.be/hs-fs/hubfs/popular-
bigindustries/team/Matthias-Vallaey.png?width=66&name=Matthias-Vallaey.png)

#### [ Matthias Vallaey ](https://www.bigindustries.be/blog/author/matthias-
vallaey)

Matthias is founder of Big Industries and a Big Data Evangelist. He has a
strong track record in the IT-Services and Software Industry, working across
many verticals. He is highly skilled at developing account relationships by
bringing innovative solutions that exceeds customer expectations. In his role
as Entrepreneur he is building partnerships with Big Data Vendors and
introduces their technology where they bring most value.

[ ](https://be.linkedin.com/in/matthias-vallaey-a82571)

## Related posts

At Big Industries, we want to share our wisdom. More info on our recent
developments, newest projects and upcoming events

[ ![PSD2 Event processor Apache
Flink](https://www.bigindustries.be/hubfs/https-
www.jotform.compsd2-regulation.png) ](https://www.bigindustries.be/blog/big-
industries-helps-retail-banks-modernise-in-a-customer-friendly-way)

######  [Big Industries Academy](https://www.bigindustries.be/blog/tag/big-
industries-academy)

#### [BIG Industries helps retail banks modernise in a customer-friendly
way](https://www.bigindustries.be/blog/big-industries-helps-retail-banks-
modernise-in-a-customer-friendly-way)

#### Use case PSD2 Event Processor with Apache Kafka and Flink

BIG Industries recently helped a retail...

[ Read more ](https://www.bigindustries.be/blog/big-industries-helps-retail-
banks-modernise-in-a-customer-friendly-way)

[ ![Building Real Time Data Pipelines with Apache
Kafka](https://www.bigindustries.be/hubfs/kafka.png)
](https://www.bigindustries.be/blog/building-real-time-data-pipelines-with-
apache-kafka)

######  [Confluent](https://www.bigindustries.be/blog/tag/confluent)

#### [Building Real Time Data Pipelines with Apache
Kafka](https://www.bigindustries.be/blog/building-real-time-data-pipelines-
with-apache-kafka)

Apache Kafka is a distributed publish-subscribe messaging system that is
designed to be fast,...

[ Read more ](https://www.bigindustries.be/blog/building-real-time-data-
pipelines-with-apache-kafka)

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

