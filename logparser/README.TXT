To use, make sure Hadoop is in your path and, optionally, HADOOP_CONF is set.

From the downloaded archive execute,

> hadoop jar logparser.jar data/apache.200.txt output

If building from source,

> gradle jar
> hadoop jar ./build/libs/logparser.jar data/apache.200.txt output

Note that if HADOOP_CONF references a cluster, the above 'output' directory will show up in HDFS.
