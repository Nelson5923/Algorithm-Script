package ParallelDijkstra;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ParallelDijkstra {
    
    public static enum ReachCounter {
	COUNT;
    }

    public static class DijkstraMap extends Mapper<LongWritable, Text, LongWritable, Text> {          
          
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            
            /* Input [NodeID] [Distance] [Prev] [AdjacentNode-Weight] ... */
            
            PDNodeWritable Node = new PDNodeWritable();        
            Node.fromString(value.toString());
            int d = Node.getDistance();
            int AdjacentNode = -1;
            int Distance = 1000000000;
            
            /* Output itself */
            
            context.write(new LongWritable(Node.getNodeID()), 
                    new Text("N:" + Node.toString()));
            
            /* Output Adjacent Node and Update Distance */
            
            String AdjacentList = Node.getList();
            StringTokenizer stk = new StringTokenizer(AdjacentList);
            while(stk.hasMoreElements()){
                StringTokenizer stk2 = new StringTokenizer(stk.nextToken(),"-");
                if(stk2.hasMoreElements())
                    AdjacentNode = Integer.parseInt(stk2.nextToken());
                if(stk2.hasMoreElements())
                    Distance = d + Integer.parseInt(stk2.nextToken());
                context.write(new LongWritable(AdjacentNode), 
                    new Text("V:" + String.valueOf(Distance) + " " + Node.getNodeID()));
            }         
  
        }
        
    }    

    public static class DijkstraReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        
    	public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            PDNodeWritable Node = new PDNodeWritable();
            Node.setNodeID((int)key.get());
            
            /* Initialize the infinite distance is necessary */
            
            int minDistance = 1000000000;
            int oldDistance = 1000000000;
            int newDistance = 1000000000;
            int Prev = -1;
            int newPrev = -1;

            /* For each new distance, find the minimum distance */
            
            for(Text s : value){
                
                StringTokenizer stk = new StringTokenizer(s.toString(),":");
                StringTokenizer stk2 = null;
     
                String valueType = stk.nextToken();
                
                /* Check the Value is Node or Distance */
                
                if(valueType.equals("N")){
                    
                    if(stk.hasMoreElements()){
                        Node.fromString(Node.getNodeID() + " " + stk.nextToken());
                        oldDistance = Node.getDistance();
                    }
                    
                }
                else if(valueType.equals("V")){
                    
                    if(stk.hasMoreElements())
                        stk2 = new StringTokenizer(stk.nextToken()); 
                    
                    if(stk2.hasMoreElements())
                        newDistance = Integer.parseInt(stk2.nextToken());
                    
                    if(stk2.hasMoreElements())
                        Prev = Integer.parseInt(stk2.nextToken());
                    
                    if(newDistance < minDistance){
                       minDistance = newDistance;
                       newPrev = Prev;
                    }
                    
                }
                
            }
            
            /* Update the node if find minimum distance */
            
            if(minDistance < oldDistance){
                if(Node.getDistance() == 1000000000)
                    context.getCounter(ParallelDijkstra.ReachCounter.COUNT).increment(1);
                Node.setDistance(minDistance);
                Node.setPrev(newPrev);
            }
            
            context.write(key, new Text(Node.toString()));
            
        }
        
    }
    
    public static class GatheringMap extends Mapper<LongWritable, Text, LongWritable, Text> {
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            
            /* Only output the reachable node */
            
            PDNodeWritable Node = new PDNodeWritable();
            Node.fromString(value.toString());
            if(Node.getDistance() < 1000000000)
                context.write(new LongWritable(Node.getNodeID()), new Text(Node.getDistance() + " " + Node.getPrev()));
            
        }
        
    }    

    public static class GatheringReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        
    	public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            for(Text s : value)
                context.write(key, s);
            
        }
        
    }
    
    public static void main(String[] args) throws Exception{
        
        /* hadoop jar [.jar file] ParallelDijkstra [infile] [outdir] [src] [iterations] */
        
        PDPreProcess.PDPreProcessDriver(args[2],args[0],"/pd-tmp-0");
        int i = 0;
        int iteration = Integer.parseInt(args[3]);
        int LastTotalReachable = 0;
        
        while(true){
            
            Configuration conf = new Configuration();

            Job job1 = new Job(conf, "ParallelDijkstra");

            job1.setJarByClass(ParallelDijkstra.class);
            job1.setMapperClass(DijkstraMap.class);
            job1.setReducerClass(DijkstraReduce.class);

            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Text.class);
            
            String inDir = "/pd-tmp-" + String.valueOf(i);
            String outDir = "/pd-tmp-" + String.valueOf(i+1);

            FileInputFormat.addInputPath(job1, new Path(inDir));
            FileOutputFormat.setOutputPath(job1, new Path(outDir));

            job1.waitForCompletion(true);
            
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/pd-tmp-" + String.valueOf(i)), true);
            
            /* Repeat until no more reachable and the iteration is finished */
            
            long TotalReachable = job1.getCounters().findCounter(ReachCounter.COUNT).getValue();
            
            i++;
            iteration--;            
            
            if(iteration <= 0){
                if(LastTotalReachable == TotalReachable){
                    break;
                }
            }
            
            LastTotalReachable = (int)TotalReachable;
            
        }

        /* Gathering Output */
        
        Configuration conf = new Configuration();
  
        Job job1 = new Job(conf, "Gathering");

        job1.setJarByClass(ParallelDijkstra.class);
        job1.setMapperClass(GatheringMap.class);
        job1.setReducerClass(GatheringReduce.class);

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path("/pd-tmp-" + String.valueOf(i)));   
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
        
        FileSystem fs = FileSystem.get(conf);       
        fs.delete(new Path("/pd-tmp-" + String.valueOf(i)), true);   
        
    }
    
}
