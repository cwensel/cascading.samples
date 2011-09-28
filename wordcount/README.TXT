To use, make sure Hadoop is in your path and, optionally, HADOOP_CONF is set.

From the downloaded archive execute,

> hadoop jar wordcount.jar data/url+page.200.txt output local

If building from source,

> gradle jar
> hadoop jar ./build/libs/wordcount.jar data/url+page.200.txt output local

Note that if HADOOP_CONF references a cluster, the above 'output' directory will show up in HDFS.
