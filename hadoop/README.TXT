To use, make sure Hadoop is in your path and, optionally, HADOOP_CONF is set. Then execute

ant -Dhadoop.home=<path to hadoop>
hadoop jar ./build/hadoop.jar data/apache.200.txt output