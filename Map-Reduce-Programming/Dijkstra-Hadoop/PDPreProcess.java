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

public class PDPreProcess {
   
    public static class PDPreProcessMap extends Mapper<LongWritable, Text, LongWritable, Text> {
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            
            StringTokenizer stk = new StringTokenizer(value.toString());
            StringBuffer sb = new StringBuffer();
            int NodeID = -1;
            
            if(stk.hasMoreElements()) 
                NodeID = Integer.parseInt(stk.nextToken());
            if(stk.hasMoreElements())
                sb.append(String.valueOf(stk.nextToken())).append("-");
            if(stk.hasMoreElements())   
                sb.append(stk.nextToken());
            
            context.write(new LongWritable(NodeID), new Text(sb.toString()));
                     
        }
        
    }

    public static class PDPreProcessReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        
        int src;
        
        public void setup(Context context) throws IOException, InterruptedException {    
                     
            Configuration conf = context.getConfiguration();
            src = Integer.parseInt(conf.get("Source"));
                    
        }        
        
    	public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            StringBuffer sb = new StringBuffer();
            if(src == key.get())
                sb.append("0").append(" ").append("-1").append(" ");
            else
                sb.append("1000000000").append(" ").append("-1").append(" ");
            
            for(Text s : value)
                sb.append(s.toString()).append(" ");
            
            context.write(key, new Text(sb.toString().trim()));
            
        }
        
    }

    public static void PDPreProcessDriver(String src, String inDir, String outDir) throws Exception{
        
        Configuration conf = new Configuration();
        conf.set("Source", src);
        
        Job job1 = new Job(conf, "PDPreProcess");
       
        job1.setJarByClass(PDPreProcess.class);
        job1.setMapperClass(PDPreProcessMap.class);
        job1.setReducerClass(PDPreProcessReduce.class);
        
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(inDir));
        FileOutputFormat.setOutputPath(job1, new Path(outDir));
        
        job1.waitForCompletion(true);
        
    }
    
}