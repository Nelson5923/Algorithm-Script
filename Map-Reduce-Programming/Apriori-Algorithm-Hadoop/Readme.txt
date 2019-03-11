/* Apriori.java */

javac Apriori.java
java Apriori [Threshold]

/* SonAlgorithm.java */

rm SonAlgorithm*.java
nano SonAlgorithm.java
hadoop fs -copyFromLocal ./son /user/1155079291/son
bin/hadoop com.sun.tools.javac.Main SonAlgorithm.java
jar cf son.jar SonAlgorithm*.class

hadoop fs -rm -r /son-output
hadoop fs -rm -r /son-temp

hadoop jar son.jar SonAlgorithm \
/son/* \
/son-temp \
/son-output \
0.4 \
2 \
9

hadoop fs -cat /son-output/*
hadoop fs -cat /son-temp/*

hadoop fs -cat /user/1155079291/son/* | wc -l
hadoop fs -cat /user/1155079291/son-output/*

hadoop fs -rm -r /user/1155079291/son-output
hadoop fs -rm -r /user/1155079291/son-temp

hadoop jar son.jar SonAlgorithm \
/user/1155079291/son/* \
/user/1155079291/son-temp \
/user/1155079291/son-output \
0.005 \
150 \
11420039

hadoop fs -cat /user/1155079291/son-output/* | sort -n | tail -n 40
hadoop fs -copyFromLocal ./son /user/1155079291/son

/* SonAlgorithmTriplet.java */

bin/hadoop com.sun.tools.javac.Main SonAlgorithmTriplet.java
jar cf son-tri.jar SonAlgorithmTriplet*.class

hadoop fs -rm -r /son-tri-output
hadoop fs -rm -r /son-tri-temp

hadoop jar son-tri.jar SonAlgorithmTriplet \
/son/* \
/son-tri-temp \
/son-tri-output \
0.2 \
2 \
9
hadoop fs -cat /son-tri-output/*

hadoop fs -rm -r /user/1155079291/son-tri-output
hadoop fs -rm -r /user/1155079291/son-tri-temp

hadoop jar son-tri.jar SonAlgorithmTriplet \
/user/1155079291/son/* \
/user/1155079291/son-tri-temp \
/user/1155079291/son-tri-output \
0.0025 \
100 \
11420039


