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

public class PRPreProcess {
    
    public static enum UserCounter {
	TOTALUSER;
    }

    public static class CountTotalMap extends Mapper<LongWritable, Text, LongWritable, Text> {          
          
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            
            StringTokenizer stk = new StringTokenizer(value.toString());
            if(stk.hasMoreElements()) {
                int NodeID = Integer.parseInt(stk.nextToken());
                context.write(new LongWritable(NodeID), new Text(""));
            }
            
        }
        
    }
    
    public static class CountTotalReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        
    	public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            context.getCounter(UserCounter.TOTALUSER).increment(1);
            context.write(key, new Text(""));
            
        }
        
    }     
    
    public static class PRPreProcessMap extends Mapper<LongWritable, Text, LongWritable, Text> {
            
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            
            StringTokenizer stk = new StringTokenizer(value.toString());
            int NodeID = 0;
            
            if(stk.hasMoreElements()) 
                NodeID = Integer.parseInt(stk.nextToken());
            if(stk.hasMoreElements())
                context.write(new LongWritable(NodeID), new Text(stk.nextToken()));
            else
                context.write(new LongWritable(NodeID), new Text(""));
            
        }
        
    }

    public static class PRPreProcessReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        
        int TotalUser;
        
        public void setup(Context context) throws IOException, InterruptedException {    
                      
          Configuration conf = context.getConfiguration();
          TotalUser = Integer.parseInt(conf.get("TotalUser"));
          
        } 
        
    	public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            StringBuffer sb = new StringBuffer();
            double PageRank = (double) 1 / (double) TotalUser;
            sb.append(String.valueOf(PageRank)).append(" ");
            for(Text s : value)
                sb.append(s.toString()).append(" ");
            
            context.write(key, new Text(sb.toString().trim()));
            
        }
        
    }    

    public static void PRPreProcessDriver(String inDir, String outDir) throws Exception{
        
        /* hadoop jar [.jar file] PageRank [alpha] [threshold] [iteration] [infile] [outdir] */
        
        Configuration conf = new Configuration();
        
        Job job0 = new Job(conf, "CountTotal");
        
        job0.setJarByClass(PRPreProcess.class);
        job0.setMapperClass(CountTotalMap.class);
        job0.setReducerClass(CountTotalReduce.class);
        
        job0.setMapOutputKeyClass(LongWritable.class);
        job0.setMapOutputValueClass(Text.class);
        job0.setOutputKeyClass(LongWritable.class);
        job0.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job0, new Path(inDir));
        FileOutputFormat.setOutputPath(job0, new Path(outDir));
        
        job0.waitForCompletion(true);

        long TotalUser = job0.getCounters().findCounter(UserCounter.TOTALUSER).getValue();
        conf.set("TotalUser", String.valueOf(TotalUser));
        
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outDir), true);
        
        Job job1 = new Job(conf, "PRPreProcess");
       
        job1.setJarByClass(PRPreProcess.class);
        job1.setMapperClass(PRPreProcessMap.class);
        job1.setReducerClass(PRPreProcessReduce.class);
        
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(inDir));
        FileOutputFormat.setOutputPath(job1, new Path(outDir));
        
        job1.waitForCompletion(true);

    }    
}
