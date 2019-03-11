rm Community*.java
nano CommunityDetection.java

bin/hadoop com.sun.tools.javac.Main CommunityDetection.java
jar cf cd.jar CommunityDetection*.class

hdfs dfs -rm -r /user/1155079291/small-output
hdfs dfs -rm -r /user/1155079291/small-temp-1
hdfs dfs -rm -r /user/1155079291/small-temp-2
hdfs dfs -copyFromLocal ./small /user/1155079291/small
hadoop fs -cat /user/1155079291/small-output/part-r-00000

hadoop jar cd.jar \
CommunityDetection /user/1155079291/small/* \
/user/1155079291/small-output \
/user/1155079291/small-temp-1 \
/user/1155079291/small-temp-2 \
3 \
3 \
2

hadoop fs -cat /user/1155079291/small/* | wc -l
hadoop fs -cat /user/1155079291/medium/* | wc -l
hadoop fs -cat /user/1155079291/large/* | wc -l
