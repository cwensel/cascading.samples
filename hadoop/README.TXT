To use, make sure Hadoop is in your path and, optionally, HADOOP_CONF is set. Then execute

ant -Dcascading.home=../cascading jar
hadoop jar ./build/logparser.jar data/apache.200.txt output

Note that if you are running a local cluster, the above 'output' directory will show up in HDFS.
