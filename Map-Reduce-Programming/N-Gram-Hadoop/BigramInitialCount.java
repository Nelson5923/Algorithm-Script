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

public class BigramInitialCount {
    
    public static class BigramInitialCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private Map<Integer, String> TempMapHead;
        private Map<Integer, String> TempMapTail;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            
            TempMapHead = new TreeMap<Integer, String>();
            TempMapTail = new TreeMap<Integer, String>();
           
        }   

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
           
            String[] line;
            String[] tmpline;
            HashMap<String, Integer> CombinerMap = new HashMap<String, Integer>();
            
            tmpline = value.toString().split("[^a-zA-Z]+");
            
            if (tmpline.length > 1){ /* Prevent Null Line */ 
            
                if(tmpline[0].equals(""))
                    line = Arrays.copyOfRange(tmpline, 1, tmpline.length);
                else
                    line = Arrays.copyOfRange(tmpline, 0, tmpline.length);
                
                /* Get the Bigram between Lines */
                
                TempMapHead.put((int)key.get(), line[0]);
                TempMapTail.put((int)key.get(), line[line.length - 1]);
                
                /* Get the Bigram within Line */

                for (int i = 0; i < line.length - 1; i++) {

                    StringBuffer Bigram = new StringBuffer();
                    Bigram.append(line[i].charAt(0) + " " + line[i+1].charAt(0));             

                    Integer Count = CombinerMap.get(Bigram.toString());
                    if(Count == null) Count = new Integer(0);
                    Count++;

                    CombinerMap.put(Bigram.toString(), Count);

                }         

                Set<String> keys = CombinerMap.keySet();
                for (String s : keys) 
                    context.write(new Text(s), new IntWritable(CombinerMap.get(s)));
                
            }
                 
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
            List<String> HeadList = new ArrayList<>(TempMapHead.values()); 
            List<String> TailList = new ArrayList<>(TempMapTail.values()); 
                        
            for (int i = 0; i < HeadList.size() - 1; i ++) {
                context.write(new Text(TailList.get(i).charAt(0) + " " + HeadList.get(i + 1).charAt(0)),
                        new IntWritable(1)); 
            }     
          
        }
    }

    public static class BigramInitialCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        
    	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
        
            for (IntWritable val : value)
		sum += val.get();
            
            context.write(key, new IntWritable(sum));
	
        }
    }
        
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        Job job1 = new Job(conf, "BigramInitialCount");

        job1.setJarByClass(BigramInitialCount.class);
        job1.setMapperClass(BigramInitialCountMap.class);
        job1.setReducerClass(BigramInitialCountReduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
    
  }
        
}
