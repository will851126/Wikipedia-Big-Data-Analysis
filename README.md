# Wikipedia Big Data Analysis

This [analysis](https://www.tableau.com/learn/articles/big-data-analytics) consists of using big data tools to answer questions about datasets from [Wikipedia](https://www.wikipedia.org/). There are a series of analysis questions, answered using [Hive](https://en.wikipedia.org/wiki/Apache_Hive) and MapReduce. The tools used are determined based on the context for each question. The output of the analysis includes [MapReduce](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html) [.jar files](https://en.wikipedia.org/wiki/JAR_(file_format)) and [.hql](https://hive.apache.org/) files so that the analysis is a repeatable process that works on a larger [dataset](https://en.wikipedia.org/wiki/Data_set), not just an [ad hoc](https://en.wikipedia.org/wiki/Ad_hoc) calculation.

## Technologies Used

1.  [Scala](https://www.scala-lang.org/)
2.  [sbt](https://www.scala-sbt.org/)
3.  [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)
4.  [YARN](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)
5.  [MapReduce](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html)
6.  [Apache Hadoop](https://hadoop.apache.org/)
7.  [Apache Hive](https://hive.apache.org/)
8.  [DBeaver](https://dbeaver.io/)

## Features

1.  Find, organize, and format pageviews on any given day.
2.  Follow clickstreams to find relative frequencies of different pages.
3.  Determine relative popularity of page access methods.
4.  Compare yearly popularity of pages.

### Getting Started

Most of the code was done using HQL in a [Hive](https://hive.apache.org/) GUI interface via [DBeaver](https://dbeaver.io/)

1. [Download DBeaver Community Edition](https://dbeaver.io/download/)
2. [Install Hive on your machine or virtual machine](https://phoenixnap.com/kb/install-hive-on-ubuntu)
3. [Clone](https://www.git-scm.com/docs/git-clone) my code - `git clone https://github.com/samye760/Wikipedia-Big-Data-Analysis.git`
4. Setup a [Hive](https://hive.apache.org/) connection in DBeaver, import my script, and start querying the data.

### Usage

1. The [HQL](https://hive.apache.org/) commands can be used on similar large datasets, specifically those found in [Wikipedia Dumps](https://dumps.wikimedia.org/)
2. This script was designed to answer all sorts of questions pertaining to [big data](https://www.oracle.com/big-data/what-is-big-data/).
