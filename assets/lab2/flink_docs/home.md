# Getting Started with Flink SQL: In-Depth Guide

In the first two parts of our Inside Flink blog series, we explored the benefits of stream processing with Flink and common Flink use cases for which teams are choosing to leverage the popular framework to unlock the full potential of streaming. Specifically, we broke down the key reasons why developers are choosing Apache Flink as their stream processing framework, as well as the ways in which they are putting it into practice. These range from streaming data pipelines to train ML models, to real-time inventory management in retail and predictive maintenance in manufacturing.

Next, we'll dive into Flink SQL, which is a powerful data processing engine that allows developers to process and analyze large volumes of data in real time. We'll cover how Flink SQL relates to the other Flink APIs and showcase some of its built-in functions and operations with syntax examples.

- Part 1: Stream Processing Simplified: An Inside Look at Flink for Kafka Users
- Part 2: Flink in Practice: Stream Processing Use Cases for Kafka Users
- Part 4: Introducing Confluent Cloud for Apache Flink

For those who want to explore Flink SQL further, we recommend checking out the Flink 101 developer course on Confluent Developer. The course provides an in-depth introduction to Apache Flink, including a detailed module on Flink SQL with practical exercises.

## Table of contents

- What is Flink SQL?
- How Flink SQL relates to other Flink APIs
- Dynamic tables and continuous queries
- The first queries to get started with Flink SQL
- Flink SQL features
  - Joins
  - Aggregations
  - Windows
  - Pattern Recognition
- Streaming vs. batch in Flink SQL
- Ready to put Flink SQL into practice?

## What is Flink SQL?

Flink SQL is an ANSI standard compliant SQL engine that can process both real-time and historical data. It provides users with a declarative way to express data transformations and analytics on streams of data.

With Flink SQL, users can easily transform and analyze data streams without having to write complex code. It supports a wide range of SQL operations, including filtering, aggregating, joining, and windowing. Flink SQL can be extended via user-defined functions (UDFs) that can be written in Java or Python. Additionally, it comes with an extensive ecosystem that includes a JDBC Driver, SQL Gateway, catalogs, and an interactive SQL shell.

Overall, Flink SQL is an easy, yet powerful solution for processing data streams using SQL syntax. It provides users with a simple and efficient way to analyze and transform data. The widespread use and familiarity of SQL allows more developers to access and work with streaming data using SQL-based tools, even if they don't have prior experience with streaming data technologies.

## How Flink SQL relates to other Flink APIs

Flink SQL is one of several APIs offered by Apache Flink for stream processing. As we mentioned in Part One, the different APIs in Flink cater to developers with varying levels of expertise and are suitable for simple to complex use cases.

Since all the APIs in Flink are interoperable, developers can use one or many APIs and switch between them as per their requirements. Flink SQL is an extremely powerful tool that can define both simple and complex queries, making it well-suited for most stream processing use cases, particularly building real-time data products and pipelines. However, in some cases where users require access to lower-level APIs for more customization, programmatic APIs, such as the DataStream API, may be recommended.

## Dynamic tables and continuous queries

In Flink SQL, a table is a structured representation of data that can be queried using SQL syntax. Tables can be created from various sources such as streams, files, or other tables. To create a table, you register metadata to connect to the system you want to use, such as Kafka. Dynamic tables process streaming data and continuously update their results to reflect changes on input tables. The underlying data of a dynamic table is stored in the storage layer, which in this example is Kafka.

Flink uses stream-table duality, allowing developers to use the same operations and functions to process both streams and tables. A stream is a record of changes in a dynamic table over time, known as a changelog. The changelog stream contains all changes, including "before" and "after" values, and can reconstruct the current state of the table at any point in time.

Understanding the types of changelogs is important because it helps Flink SQL users to design their jobs in a way that ensures fault tolerance and consistency of their data processing.

Flink provides four different types of changelog entries:

| Type     | Meaning        | Description                                                                                     |
|----------|----------------|-------------------------------------------------------------------------------------------------|
| `+I`     | Insertion      | This records only the insertions that occurs.                                                   |
| `-U`     | Update Before  | This retracts a previously emitted result.                                                      |
| `+U`     | Update After   | This updates a previously emitted result. This requires a primary key if `-U` is omitted.       |
| `-D`     | Delete         | This deletes the last result.                                                                   |

Flink's changelogs have various uses depending on the application. For example, Insertion changelogs track new orders, Update Before changelogs track stock price changes, Update After changelogs track updated order amounts, and Delete changelogs track canceled orders. Flink generates the appropriate changelog based on the query but may require manual specification in some cases.

Depending on the combination of source, sink, and business logic applied, you can end up with the following types of streams:

## The first queries to get started with Flink SQL

The first step of data processing requires you to connect to your source system by defining a table. The Flink Catalog provides a unified metadata management system for Flink's Table API and SQL. It is a logical namespace that contains metadata information about data sources, such as Apache Kafka, file systems, and databases. The Flink Catalog enables users to define and manage data sources, tables, and views that can be queried using SQL.

For example, here's how you can register the metadata in Flink to connect to your Kafka topic `transactions`:

If you want to store the result of your data product, such as the revenue per user, you would first register appropriate metadata, like this:

You can find all security configuration options in the Flink documentation.

Secondly, write the desired business logic in your SQL statement, like this:

When using Flink on Confluent Cloud, you can start writing your business logic directly, since all of the Confluent Cloud metadata is automatically available and ready to use.

## Flink SQL features

Flink SQL is compliant with ANSI SQL standards, making it easy for those familiar with relational databases to learn. It offers a wide range of SQL features and functions, including support for joins, aggregations, windows, and more. With Flink's unified API for batch and stream processing, you can apply these features to both bounded and unbounded data, allowing users to write complex queries that handle a variety of data processing tasks.

Flink SQL applies advanced optimization techniques, such as query optimization and cost-based optimization, to ensure queries are executed efficiently and with minimal resource usage. This allows users to process large volumes of data quickly and efficiently, even in complex data processing scenarios.

### Joins

Flink SQL offers a variety of joins, including inner joins and outer joins (left, right, and full), cross joins, and other special joins. Each of these joins follows the conventional semantics for its respective operation.

#### Inner and Outer joins

In Flink SQL, inner and outer joins are used to combine rows from two or more tables based on a common column between them.

- **Inner join:** Returns only the matching rows from both tables
- **Left outer join:** Returns all the rows from the left table and matching rows from the right table, or null values if there is no match
- **Right outer join:** Returns all the rows from the right table and matching rows from the left table, or null values if there is no match
- **Full outer join:** Returns all the rows from both tables, or null values if there is no match

Here's an example query that illustrates how to use inner and outer joins:

This query calculates the total amount of orders placed by brand and store in a specific quarter. It first joins the Orders table with the DateDim table on the shared column "DateID". It then performs left outer joins with the Store and Product tables on the common columns "StoreID" and "ProductID", respectively. Note that right outer joins could have been used instead. The result is then grouped by brand, store, and quarter using the `GROUP BY` clause.

#### Cross join

In Flink SQL, a cross join is a type of join that returns the Cartesian product of the two tables being joined. The Cartesian product is a combination of every row from the first table with every row from the second table. This feature can be particularly useful when you need to expand an array column into multiple rows.

#### Interval join

An interval join is another type of join that allows users to join two streams or tables based on a time interval or range. In an interval join, the join condition is based on a time attribute, and the join result includes all rows that fall within the specified time interval or range.

For instance, this query will join all orders with their corresponding shipments if the order was shipped four hours after it was received.

#### Temporal join

A temporal join is a type of join that enables users to join two streams or tables based on a temporal relationship between the records. In a temporal join, the join condition is based on a time attribute, and the join result includes all rows that satisfy the temporal relationship. A common use case for temporal joins is analyzing financial data, which often includes information that changes over time, such as stock prices, interest rates, and exchange rates.

#### Lateral join

A lateral join in Flink SQL is a type of join that allows you to apply a table-valued function to each row of a table and generate additional rows based on the function's output. Lateral joins are useful for scenarios where you need to split a column into multiple rows or generate additional rows based on complex calculations or queries. 

Suppose you have a table called `order_items` that contains information about the items in each order, and you want to find the top 2 items with the highest price per order. You can use a lateral join to join the orders table with the `order_items` table and get the top 2 items with the highest price for each order.

### Aggregations

In addition to joining data, there is often a need to perform various aggregations. For example, the first Flink SQL query in this blog post already used a GROUP BY aggregation.

#### OVER aggregation

The OVER aggregation (covered in Part Two) is a critical tool for analyzing streaming data over time windows. Unlike typical window aggregates in a SQL database, `OVER` uses a sliding or tumbling window of rows over a specified time period. This makes it ideal for processing and analyzing constantly changing data streams.

Additionally, the `OVER` clause can define time-based windows with the help of a watermark to track the progress of time in a data stream. By using the OVER aggregation, users can perform complex calculations and aggregations on streaming data in real time, gaining valuable insights into emerging trends and patterns.

For instance, suppose we have a table of sales data that includes the product ID, sales date, and sales amount. We can use an OVER aggregation to calculate the rolling sum of sales for each product over a sliding window of the past 7 days.

#### TopN aggregation

Another type of aggregation is the TopN aggregation, which enables users to find the top N values for a given column within a sliding or tumbling window of rows. The TopN function is applied to the window of rows and returns the top N rows based on the specified column and ordering.

For instance, suppose we have a table of website traffic data that includes the page URL, visitor IP address, and number of visits. We can use a TopN aggregation to find the top 10 most visited pages over a sliding window of the past 24 hours.

Here's an example of what the SQL query might look like:

### Windows

In addition to aggregations, Flink also provides various ways to window data. A window is a logical grouping of rows based on a time or key attribute that is used to define a subset of the data over which an aggregation will be performed. Flink's table valued functions offer tumbling, hopping, or cumulative windows:

#### Tumbling window

Tumbling windows divide data into non-overlapping, fixed-size windows. This is useful when you want to analyze data in discrete time intervals. For instance, this SQL statement uses a tumbling window to group the data in the "orders" table based on a fixed-size time interval of 10 minutes.

#### Hopping window

A hopping window is a type of window that groups rows based on a sliding time interval. Unlike a tumbling window, which groups rows into non-overlapping fixed-size windows, a hopping window groups rows into overlapping windows of a fixed size and with a specified slide interval.

This can be useful when analyzing time-series data, where you want to see how a certain metric changes over time in a more granular way than a tumbling window would allow. For example, this query uses a hopping window to group the rows in the orders table into overlapping windows of 5 minutes with a slide interval of 10 minutes:

#### Cumulative window

A cumulative window is a type of window that groups rows based on a cumulative condition. Unlike tumbling or hopping windows, which group rows based on fixed-size time intervals, a cumulative window groups rows based on a condition that accumulates over time.

An example of when you would use a cumulative window is when you want to calculate a running total or a rolling average of a certain metric over time. For instance, this query calculates the cumulative sales for each order in the orders table over a window size of 2 minutes and a slide interval of 10 minutes:

### Pattern recognition

During my keynote demo at KSL 2023, I talked about pattern recognition, which is a crucial aspect of data analysis. One of the powerful features that Flink SQL offers for pattern recognition is `MATCH_RECOGNIZE`. This feature enables pattern matching on a stream of data, allowing you to specify a pattern of events that you want to detect within the stream. You can then perform various calculations or actions based on that pattern.

Here's an example of what the SQL query might look like:

In this example, the `MATCH_RECOGNIZE` statement is used to detect patterns in a stream of stock price data. The `PARTITION BY` clause is used to group the data by stock symbol, and the `ORDER BY` clause is used to order the data by event time.

The `MEASURES` clause is used to define the measures that we want to calculate based on the pattern. In this case, we're calculating the first and last prices in each pattern, as well as the average price.

The `PATTERN` clause is used to define the pattern that we want to detect. In this case, we're looking for patterns that start with one or more upward price movements, followed by one or more downward price movements, and end with a downward movement.

The `DEFINE` clause is used to define the conditions that must be met for each part of the pattern. In this case, an upward movement is defined as a price that is higher than the previous price, and a downward movement is defined as a price that is lower than the previous price.

## Streaming vs. batch in Flink SQL

Flink SQL supports both streaming and batch processing modes, allowing developers to use the same SQL queries for both batch and streaming data processing without the need for rewriting code.

In streaming mode, Flink SQL processes continuous streams of data in real time, and only supports sorting by time. This means you can use `ORDER BY` to sort data based on a time attribute of each record. In batch mode, Flink SQL processes static datasets that do not change over time, and supports sorting by any column using `ORDER BY`.

Flink SQL's streaming mode has optimizations for temporal joins that take advantage of the time-based nature of the data, making them more efficient than regular joins. However, batch mode does not have these optimizations, so regular joins are used instead. This can be less efficient for streaming data.

When choosing between streaming and batch processing modes in Flink SQL, consider the nature of your data and the type of processing you need to perform. Streaming mode is ideal for real-time processing of continuous data, while batch mode is best suited for processing static datasets. For example, use streaming mode for use cases such as real-time fraud detection in financial transactions, and use batch mode for generating financial reports on a daily, weekly, or monthly basis.

Each mode has its own set of features and limitations, so it's important to choose the mode that best fits your use case.

## Ready to put Flink SQL into practice?

Flink SQL is a powerful tool for processing data streams with SQL syntax, suitable for real-time data products or generating reports from static datasets. It offers features and capabilities for a wide range of use cases. To explore Flink SQL further, try our Flink 101 developer course.

We're excited to share the amazing lineup of talks on Flink and Kafka at the Current 2023 data streaming conference in San Jose on September 26-27th. Learn from top Flink experts and gain valuable insights into the latest trends and best practices in data streaming. Register now to secure your spot at the conference!

Our next blog post will explore making Flink cloud-native, including the benefits and factors to consider when deploying Flink in a cloud-native environment. Whether you're new to cloud-native design principles or a seasoned expert, this post will provide valuable insights and practical advice for running Flink in the cloud. Keep an eye out for the next post in our series!

---

**Related posts:**

- [Stream Processing Simplified: An Inside Look at Flink for Kafka Users](https://www.confluent.io/blog/stream-processing-simplified-inside-look-at-flink-for-kafka-users/)  
  Konstantin Knauf

- [Introducing Confluent Cloud for Apache Flink](https://www.confluent.io/blog/introducing-confluent-cloud-for-apache-flink/)  
  James Rowland-Jones (JRJ)