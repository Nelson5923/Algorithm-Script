import java.io.*;
import java.util.*;

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

public class WordLengthCount {
    
    public static class WordLengthCountMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        
        public Map<Integer, Integer> CombinerMap;
        
        public void setup(Context context) throws IOException, InterruptedException {
            
            CombinerMap = new HashMap<Integer, Integer>();
           
        }     
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
            String[] line = value.toString().split(" ");
            
            if(line.length > 0){
                
                for(String s : line) {

                    int WordLength = s.length();
                    Integer Count = CombinerMap.get(WordLength);
                    if(Count == null) Count = new Integer(0);
                    Count++;
                    CombinerMap.put(WordLength, Count);
                    
                }
                
            }
            
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
            
            ArrayList<Integer> Key = new ArrayList<Integer>(CombinerMap.keySet());

            for (Integer d : Key) 
                context.write(new IntWritable(d), new IntWritable(CombinerMap.get(d)));
          
        }
        
    }

    public static class WordLengthCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        
    	public void reduce(IntWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
        
            for (IntWritable val : value)    
		sum += val.get();
            
            context.write(key, new IntWritable(sum));
	
        }
    }
        
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        Job job1 = new Job(conf, "WordLengthCount");

        job1.setJarByClass(WordLengthCount.class);
        job1.setMapperClass(WordLengthCountMap.class);
        job1.setReducerClass(WordLengthCountReduce.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
    
  }
    
}
