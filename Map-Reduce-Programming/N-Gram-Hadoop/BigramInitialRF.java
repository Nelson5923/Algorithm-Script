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

public class BigramInitialRF {
    
    public static String getValue(Text s) {

        StringTokenizer itr = new StringTokenizer(s.toString());
        itr.nextToken();
        return itr.nextToken();
        
    }
    
    public static String getKey(Text s) {

        StringTokenizer itr = new StringTokenizer(s.toString());
        return itr.nextToken();
        
    }
    
    public static class BigramInitialRFMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        
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
            
            /* Parsing the Text */
            
            tmpline = value.toString().split("[^a-zA-Z]+");
            
            if (tmpline.length > 1){
                
                if(tmpline[0].equals(""))
                    line = Arrays.copyOfRange(tmpline, 1, tmpline.length);
                else
                    line = Arrays.copyOfRange(tmpline, 0, tmpline.length);

                /* Count the Bigram between Lines */

                TempMapHead.put((int)key.get(), line[0]);
                TempMapTail.put((int)key.get(), line[line.length - 1]);

                /* Count the Bigram */

                for (int i = 0; i < line.length - 1; i++) {

                    StringBuffer Bigram = new StringBuffer();
                    Bigram.append(line[i].charAt(0) + " " + line[i+1].charAt(0));
                    
                    Integer Count = CombinerMap.get(Bigram.toString());
                    if(Count == null) Count = new Integer(0);
                    Count++;
                    
                    CombinerMap.put(Bigram.toString(), Count);

                }         

                /* Output to the Reducer */

                Set<String> keys = CombinerMap.keySet();
                for (String s : keys) {
                    context.write(new Text(s), new IntWritable(CombinerMap.get(s)));                              
                    context.write(new Text(getKey(new Text(s)) + " " + "*"), new IntWritable(CombinerMap.get(s)));
                }
                
            }
            
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
            /* Output the Bigram between Lines to the Reducers */
            
            List<String> HeadList = new ArrayList<>(TempMapHead.values()); 
            List<String> TailList = new ArrayList<>(TempMapTail.values()); 
                        
            for (int i = 0; i < HeadList.size() - 1; i ++) {
                context.write(new Text(TailList.get(i).charAt(0) + " " + HeadList.get(i + 1).charAt(0)), new IntWritable(1));
                context.write(new Text(TailList.get(i).charAt(0) + " " + "*"), new IntWritable(1));
            }     
          
        }
        
    }
       
    public static class PairPartitioner extends Partitioner<Text,IntWritable> {
        
        /* Pair with Same Key Distributed to the Same Pair */
        
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {                       
            return getKey(key).hashCode() % numPartitions;
        }
        
    }
    
    public static class KeyComparator extends WritableComparator {
  
        /* Create a Sorting Such That (W, *) is alway atop */
        public int compare(Text w1, Text w2) {         
            
            int val = getKey(w1).compareTo(getKey(w2));
            
            if(val != 0)
                return val;
            else if(getValue(w1).equals("*"))
                return -1;
            else if(getValue(w2).equals("*"))
                return 1;
            else
                return getValue(w1).compareTo(getValue(w2));
 
        }
  
    }
    
    public static class BigramInitialRFReduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        
        HashMap<String, Integer> MarginalCount;
        private double N;
        
        protected void setup(Context context) throws IOException, InterruptedException {    
            
            /* Get the Parameter */
            
            Configuration conf = context.getConfiguration();
            String theta = conf.get("theta");
            N = Double.parseDouble(theta);
            MarginalCount = new HashMap<String, Integer>();
            
            //System.out.printf("%d\n",N);
        
        }
        
    	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            double RelativeFrequency;
            
            for (IntWritable val : value) {
		sum += val.get();            
            }
            
            if(getValue(key).equals("*"))
                MarginalCount.put(getKey(key), sum);
            else {               
                RelativeFrequency = (double)sum / (double)MarginalCount.get(getKey(key));
                if (RelativeFrequency >= N) 
                    context.write(new Text(key.toString()), new DoubleWritable(RelativeFrequency));
            }
   
        }
        
    }
        
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        conf.set("theta", args[2]);
        
        Job job1 = new Job(conf, "BigramInitialRF");       

        job1.setJarByClass(BigramInitialRF.class);
        job1.setMapperClass(BigramInitialRFMap.class);
        job1.setReducerClass(BigramInitialRFReduce.class);
        job1.setPartitionerClass(PairPartitioner.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
    
  }
        
}

