/* WordLengthCount */

hadoop fs -rm -r /wlc-output
hadoop fs -rm -r /wlc-temp

bin/hadoop jar wlc.jar \
WordLengthCount /wlc/* \
/wlc-output

bin/hadoop com.sun.tools.javac.Main WordLengthCount.java
jar cf wlc.jar WordLengthCount*.class

hadoop fs -cat /wlc-output/part-r-00000
hadoop fs -copyFromLocal ./wlc /wlc

mkdir wlc
cd ./wlc
nano wlc.txt

The quick brown fox jumps
over the lazy dog.

/* BigramInitialCount */

hadoop fs -rm -r /bic-output
hadoop fs -rm -r /bic-temp

bin/hadoop jar bic.jar \
BigramInitialCount /bic/* \
/bic-output

bin/hadoop com.sun.tools.javac.Main BigramInitialCount.java
jar cf bic.jar BigramInitialCount*.class

hadoop fs -cat /bic-output/part-r-00000
hadoop fs -copyFromLocal ./bic /bic

mkdir bic
cd ./bic
nano bic.txt

"The quick brown fox jumps
over the lazy dog."

/* BigramRelativeFrequency */

hadoop fs -rm -r /brf-output
hadoop fs -rm -r /brf-temp

bin/hadoop jar brf.jar \
BigramInitialRF /brf/* \
/brf-output 0.6

bin/hadoop com.sun.tools.javac.Main BigramInitialRF.java
jar cf brf.jar BigramInitialRF*.class

hadoop fs -cat /brf-output/part-r-00000
hadoop fs -copyFromLocal ./brf /brf

mkdir brf
cd ./brf
nano brf.txt

the quick brown fox jumps over the lazy dogs.
the Quick Brown fox jumps Over the Lazy dogs.
the Quick Brown fox Jumps Over the Lazy dogs?


