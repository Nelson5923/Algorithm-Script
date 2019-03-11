/* hadoop jar [.jar file] PageRank [alpha] [threshold] [iteration] [infile] [outdir] */

hadoop fs -copyFromLocal ./pr /pr

cd ./PageRank
hadoop com.sun.tools.javac.Main *.java
cd ../
jar cf pr.jar PageRank/*.class

hadoop fs -rm -r /pr-*

hadoop jar pr.jar PageRank/PageRank \
0.2 \
0.167 \
30 \
/pr/* \
/pr-out

hadoop fs -cat /pr-out/*