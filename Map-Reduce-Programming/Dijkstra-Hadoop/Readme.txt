hadoop fs -copyFromLocal ./pd /pd

hadoop jar [.jar file] ParallelDijkstra [infile] [outdir] [src] [iterations]

cd ./ParallelDijkstra
hadoop com.sun.tools.javac.Main *.java
cd ../
jar cf pd.jar ParallelDijkstra/*.class

hadoop fs -rm -r /pd-*

hadoop jar pd.jar ParallelDijkstra/ParallelDijkstra \
/pd/* \
/pd-out \
1 \
1

hadoop fs -cat /pd-out/*

Sample Input:
1 2 7
1 3 20
2 3 3
3 1 5
4 1 9
5 6 10

Sample Output:
1 0 nil
2 7 1
3 10 2
