To use, make sure Hadoop is in your path and, optionally, HADOOP_CONF is set. Then execute

ant jar
hadoop jar ./build/wordcount.jar data/url+page.200.txt output local