[ ![Apache Flink](/img/logo/png/100/flink_squirrel_100_color.png) Apache Flink
](/) __

  * About
    * [Architecture](/what-is-flink/flink-architecture/)
    * [Applications](/what-is-flink/flink-applications/)
    * [Operations](/what-is-flink/flink-operations/)
    * [Use Cases](/what-is-flink/use-cases/)
    * [Powered By](/what-is-flink/powered-by/)
    * [Roadmap](/what-is-flink/roadmap/)
    * [Community & Project Info](/what-is-flink/community/)
    * [Security](/what-is-flink/security/)
    * [Special Thanks](/what-is-flink/special-thanks/)
  * Getting Started
    * [With Flink __](https://nightlies.apache.org/flink/flink-docs-stable/docs/try-flink/local_installation/)
    * [With Flink Kubernetes Operator __](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/try-flink-kubernetes-operator/quick-start/)
    * [With Flink CDC __](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/get-started/introduction/)
    * [With Flink Agents __](https://nightlies.apache.org/flink/flink-agents-docs-latest/docs/get-started/overview/)
    * [With Flink ML __](https://nightlies.apache.org/flink/flink-ml-docs-stable/docs/try-flink-ml/quick-start/)
    * [Training Course __](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/overview/)
    * [With Flink Stateful Functions __](https://nightlies.apache.org/flink/flink-statefun-docs-stable/getting-started/project-setup.html)
  * Documentation
    * [Flink 2.2 (stable)__](https://nightlies.apache.org/flink/flink-docs-stable/)
    * [Flink 1.20 (LTS)__](https://nightlies.apache.org/flink/flink-docs-lts/)
    * [Flink Master (snapshot)__](https://nightlies.apache.org/flink/flink-docs-master/)
    * [Kubernetes Operator 1.15 (latest)__](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/)
    * [Kubernetes Operator Main (snapshot)__](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main)
    * [CDC 3.6 (stable)__](https://nightlies.apache.org/flink/flink-cdc-docs-stable)
    * [CDC Master (snapshot)__](https://nightlies.apache.org/flink/flink-cdc-docs-master)
    * [Agents 0.2 (latest)__](https://nightlies.apache.org/flink/flink-agents-docs-latest/)
    * [Agents Main (snapshot)__](https://nightlies.apache.org/flink/flink-agents-docs-main/)
    * [ML 2.3 (stable)__](https://nightlies.apache.org/flink/flink-ml-docs-stable/)
    * [ML Master (snapshot)__](https://nightlies.apache.org/flink/flink-ml-docs-master)
    * [Stateful Functions 3.3 (stable)__](https://nightlies.apache.org/flink/flink-statefun-docs-stable/)
    * [Stateful Functions Master (snapshot)__](https://nightlies.apache.org/flink/flink-statefun-docs-master)
  * How to Contribute
    * [Overview](/how-to-contribute/overview/)
    * [Contribute Code](/how-to-contribute/contribute-code/)
    * [Review Pull Requests](/how-to-contribute/reviewing-prs/)
    * [Code Style and Quality Guide](/how-to-contribute/code-style-and-quality-preamble/)
    * [Contribute Documentation](/how-to-contribute/contribute-documentation/)
    * [Documentation Style Guide](/how-to-contribute/documentation-style-guide/)
    * [Contribute to the Website](/how-to-contribute/improve-website/)
    * [Getting Help](/how-to-contribute/getting-help/)
  * [Flink Blog](/posts/)
  * [Downloads](/downloads/)

__

____

#  What is Apache Flink? â Architecture #

Apache Flink is a framework and distributed processing engine for stateful
computations over _unbounded and bounded_ data streams. Flink has been
designed to run in _all common cluster environments_ , perform computations at
_in-memory speed_ and at _any scale_.

Here, we explain important aspects of Flink's architecture.

##  Process Unbounded and Bounded Data #

Any kind of data is produced as a stream of events. Credit card transactions,
sensor measurements, machine logs, or user interactions on a website or mobile
application, all of these data are generated as a stream.

Data can be processed as _unbounded_ or _bounded_ streams.

  1. **Unbounded streams** have a start but no defined end. They do not terminate and provide data as it is generated. Unbounded streams must be continuously processed, i.e., events must be promptly handled after they have been ingested. It is not possible to wait for all input data to arrive because the input is unbounded and will not be complete at any point in time. Processing unbounded data often requires that events are ingested in a specific order, such as the order in which events occurred, to be able to reason about result completeness.

  2. **Bounded streams** have a defined start and end. Bounded streams can be processed by ingesting all data before performing any computations. Ordered ingestion is not required to process bounded streams because a bounded data set can always be sorted. Processing of bounded streams is also known as batch processing.

![](https://flink.apache.org//img/bounded-unbounded.png)

**Apache Flink excels at processing unbounded and bounded data sets.** Precise
control of time and state enable Flink's runtime to run any kind of
application on unbounded streams. Bounded streams are internally processed by
algorithms and data structures that are specifically designed for fixed sized
data sets, yielding excellent performance.

Convince yourself by exploring the [use cases](/what-is-flink/use-cases/) that
have been built on top of Flink.

##  Deploy Applications Anywhere #

Apache Flink is a distributed system and requires compute resources in order
to execute applications. Flink integrates with all common cluster resource
managers such as [Hadoop YARN](https://hadoop.apache.org/docs/stable/hadoop-
yarn/hadoop-yarn-site/YARN.html) and [Kubernetes](https://kubernetes.io/) but
can also be setup to run as a stand-alone cluster.

Flink is designed to work well with each of the previously listed resource
managers. This is achieved by resource-manager-specific deployment modes that
allow Flink to interact with each resource manager in its idiomatic way.

When deploying a Flink application, Flink automatically identifies the
required resources based on the application's configured parallelism and
requests them from the resource manager. In case of a failure, Flink replaces
the failed container by requesting new resources. All communication to submit
or control an application happens via REST calls. This eases the integration
of Flink in many environments.

##  Run Applications at any Scale #

Flink is designed to run stateful streaming applications at any scale.
Applications are parallelized into possibly thousands of tasks that are
distributed and concurrently executed in a cluster. Therefore, an application
can leverage virtually unlimited amounts of CPUs, main memory, disk and
network IO. Moreover, Flink easily maintains very large application state. Its
asynchronous and incremental checkpointing algorithm ensures minimal impact on
processing latencies while guaranteeing exactly-once state consistency.

[Users reported impressive scalability numbers](/what-is-flink/powered-by/)
for Flink applications running in their production environments, such as

  * applications processing **multiple trillions of events per day** ,
  * applications maintaining **multiple terabytes of state** , and
  * applications **running on thousands of cores**.

##  Leverage In-Memory Performance #

Stateful Flink applications are optimized for local state access. Task state
is always maintained in memory or, if the state size exceeds the available
memory, in access-efficient on-disk data structures. Hence, tasks perform all
computations by accessing local, often in-memory, state yielding very low
processing latencies. Flink guarantees exactly-once state consistency in case
of failures by periodically and asynchronously checkpointing the local state
to durable storage.

![](https://flink.apache.org//img/local-state.png)

[Want to contribute
translation?](https://cwiki.apache.org/confluence/display/FLINK/Flink+Translation+Specifications)

[ Edit This Page __](//github.com/apache/flink-web/edit/asf-
site/docs/content/what-is-flink/flink-architecture.md)

### On This Page [__](javascript:void\(0\))

  * What is Apache Flink? â Architecture
    * Process Unbounded and Bounded Data
    * Deploy Applications Anywhere
    * Run Applications at any Scale
    * Leverage In-Memory Performance

[ __](javascript:void\(0\))

  * [flink-packages.org](https://flink-packages.org/)
  * [Apache Software Foundation](https://www.apache.org/)
  * [License](https://www.apache.org/licenses/)
  * [ __  ä¸­æç ](/zh/what-is-flink/flink-architecture/)

  * [Security
  * [Donate](https://www.apache.org/foundation/sponsorship.html)
  * [Thanks](https://www.apache.org/foundation/thanks.html)

[ Flink blog ](/posts) [ Github ](https://github.com/apache/flink) [ Twitter
](https://twitter.com/apacheflink)

* * *

The contents of this website are Â© 2024 Apache Software Foundation under the
terms of the Apache License v2. Apache Flink, Flink, and the Flink logo are
either registered trademarks or trademarks of The Apache Software Foundation
in the United States and other countries.

