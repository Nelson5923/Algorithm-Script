package PageRank;
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

public class PageRank {
    
    public static enum PageRankCounter {
	PAGERANK;
    }
    
    public static enum UserCounter {
	TOTALUSER;
    }

    public static class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {          
          
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            
            /* Input: [NodeID] [PageRank] [AdjacentList] */
            
            StringTokenizer stk = new StringTokenizer(value.toString());
            PRNodeWritable Node = new PRNodeWritable();
            
            /* Parse the String to Node */
            
            if(stk.hasMoreElements())
                Node.setNodeID(Integer.parseInt(stk.nextToken()));
            if(stk.hasMoreElements())
                Node.setPageRank(Double.parseDouble(stk.nextToken()));
            while(stk.hasMoreElements()) {
                Node.addList(Integer.parseInt(stk.nextToken()));
            }
            
            /* Output the Node itself */
            
            context.write(new LongWritable(Node.getNodeID()), 
                    new Text("N " + Node.getPageRank() + " " + Node.getList()));
            
            /* Calculate the PageRank and Output Adjacent Node */
            
            double p = 0;
            if ((double)Node.getListLength() > 0)
                p = Node.getPageRank() / (double)Node.getListLength();
            stk = new StringTokenizer(Node.getList());
            while(stk.hasMoreElements()) {
                context.write(new LongWritable(Long.parseLong(stk.nextToken())), new Text("V " + String.valueOf(p)));
            }
            
        }
        
    }    

    public static class PageRankReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        
    	public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            PRNodeWritable Node = new PRNodeWritable();
            Node.setNodeID((int)key.get());
            double PageRank = 0;
            
            for(Text s : value){
                
                StringTokenizer stk = new StringTokenizer(s.toString());
                
                String valueType = stk.nextToken();
                
                /* Check the Key is Node or PageRank Value */
                
                if(valueType.equals("N")){
                    
                    if(stk.hasMoreElements())
                        Node.setPageRank(Double.parseDouble(stk.nextToken()));
                    while(stk.hasMoreElements())
                        Node.addList(Integer.parseInt(stk.nextToken()));
                    
                }
                else if(valueType.equals("V")){
                    
                    PageRank =  PageRank + Double.parseDouble(stk.nextToken());
                    
                }        
 
            }
                 
            Node.setPageRank(PageRank);
            
            /* Count the Missing PageRank */
            
            context.getCounter(PageRankCounter.PAGERANK).increment((long)(PageRank * 1000000000));
            context.getCounter(UserCounter.TOTALUSER).increment(1);
            context.write(key, new Text(Node.getPageRank() + " " + Node.getList()));
            
        }
        
    }
    
    public static class GatheringMap extends Mapper<LongWritable, Text, LongWritable, Text> {
        
        double t;

        public void setup(Context context) throws IOException, InterruptedException {    
                      
            Configuration conf = context.getConfiguration();
            t = Double.parseDouble(conf.get("threshold"));      
            
        } 
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            
            StringTokenizer stk = new StringTokenizer(value.toString());
            PRNodeWritable Node = new PRNodeWritable();
            
            if(stk.hasMoreElements())
                Node.setNodeID(Integer.parseInt(stk.nextToken()));
            if(stk.hasMoreElements())
                Node.setPageRank(Double.parseDouble(stk.nextToken()));
            
            if(Node.getPageRank() > t){
                context.write(new LongWritable(Node.getNodeID()), 
                    new Text(String.valueOf(Node.getPageRank())));
            }
            
        }
        
    }    

    public static class GatheringReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        
    	public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            for(Text s : value)
                context.write(key, s);
            
        }
        
    }
    
    public static void main(String[] args) throws Exception{
        
        /* hadoop jar [.jar file] PageRank [alpha] [threshold] [iteration] [infile] [outdir] */
        
        PRPreProcess.PRPreProcessDriver(args[3],"/pr-tmp-0-Adjust");
        
        for(int i = 0; i < Integer.parseInt(args[2]); i++){
            
            Configuration conf = new Configuration();

            Job job1 = new Job(conf, "PageRank");

            job1.setJarByClass(PageRank.class);
            job1.setMapperClass(PageRankMap.class);
            job1.setReducerClass(PageRankReduce.class);

            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, new Path("/pr-tmp-" + String.valueOf(i) + "-Adjust"));
            FileOutputFormat.setOutputPath(job1, new Path("/pr-tmp-" + String.valueOf(i+1)));

            job1.waitForCompletion(true);
            
            long TotalPageRank = job1.getCounters().findCounter(PageRankCounter.PAGERANK).getValue();
            long TotalUser = job1.getCounters().findCounter(UserCounter.TOTALUSER).getValue(); 
            
            PRAdjust.PRAdjustDriver(Double.parseDouble(args[0]), TotalUser, 
                TotalPageRank, "/pr-tmp-" + String.valueOf(i+1) , "/pr-tmp-" + String.valueOf(i+1) + "-Adjust");
                       
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/pr-tmp-" + String.valueOf(i) + "-Adjust"), true);
            fs.delete(new Path("/pr-tmp-" + String.valueOf(i+1)), true);            
                      
        }

        /* Gathering Output */
        
        Configuration conf = new Configuration();
        conf.set("threshold", args[1]);

        Job job1 = new Job(conf, "Gathering");

        job1.setJarByClass(PageRank.class);
        job1.setMapperClass(GatheringMap.class);
        job1.setReducerClass(GatheringReduce.class);

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path("/pr-tmp-" + args[2] + "-Adjust"));   
        FileOutputFormat.setOutputPath(job1, new Path(args[4]));

        job1.waitForCompletion(true);
        FileSystem fs = FileSystem.get(conf);       
        fs.delete(new Path("/pr-tmp-" + args[2] + "-Adjust"), true);   
        
    }
    
}
