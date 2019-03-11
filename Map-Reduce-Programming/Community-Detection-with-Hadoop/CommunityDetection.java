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

public class CommunityDetection {
      
    public static class PreprocessingMap extends Mapper<LongWritable, Text, Text, Text> {
        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            /* Parse the Key-Value Pair / Key is the Line Number by Default */
            
            String line = value.toString();
            String[] lineArray = line.split(":");
            
            String Followee = lineArray[0];
            String Follower = lineArray[1].trim();

            String[] FollowerArray = Follower.split(" ");
            Arrays.sort(FollowerArray);
            
            /* Inverse the Followee-Follower Pair */

            for(int i = 0; i < FollowerArray.length; i++)
                context.write(new Text(FollowerArray[i]), new Text(Followee));
            
            /* Find the Pair that have Common Followee */

            for(int i = 0; i < FollowerArray.length - 1; i++){
                for(int j = i + 1; j < FollowerArray.length; j++){
                    String Pair1 = "[" + FollowerArray[i] + "-" + FollowerArray[j] + "]";
                    String Pair2 = "[" + FollowerArray[j] + "-" + FollowerArray[i] + "]";
                    context.write(new Text(FollowerArray[i]), new Text(Pair1));
                    context.write(new Text(FollowerArray[j]), new Text(Pair2));
                }
            }
                
        }
    }

    public static class PreprocessingReduce extends Reducer<Text, Text, Text, Text> {
               
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            StringBuffer RecordBuffer = new StringBuffer();           
            Set<String> Record = new HashSet();
            
            /* Use SET to remove the Duplicate */
            
            for(Text s : value)
                Record.add(s.toString());    
            
            for(String s : Record)
                RecordBuffer.append(s + " ");
                
            context.write(key, new Text(RecordBuffer.toString()));
            
        }
        
    }    
    
    public static class CommonFriendMap extends Mapper<Text, Text, Text, Text> { 
        
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            
            /* Parse the Key-Value Pair */ 
            
            String line = value.toString();           
            String[] Followees = line.replaceAll("\\[(.*?)\\]","").trim().split("\\s+");
            Arrays.sort(Followees);
            
            StringBuffer FormattedFollowees = new StringBuffer();
            for (String Followee : Followees)
                FormattedFollowees.append(Followee + " ");
            
            ArrayList<String> PairsArray  = new ArrayList();
            Matcher PairsMatch = Pattern.compile("\\[(.*?)\\]").matcher(line);
        
            while (PairsMatch.find()) {
                PairsArray.add(PairsMatch.group());
            }
            
            /* Generate the Pairs that have Common Followee as Key */
        
            for(String s : PairsArray){
                
                String[] SortedPairs = s.replaceAll("[\\[\\-\\]]"," ").trim().split(" ");
                StringBuffer FormattedPairs = new StringBuffer(""); 
                Arrays.sort(SortedPairs);
                
                for(String t : SortedPairs)
                    FormattedPairs.append(t + " ");
                
                context.write(new Text(FormattedPairs.toString().trim().replaceAll(" ",": "))
                        , new Text(FormattedFollowees.toString().trim()));
                
            }
            
        }
    }
    
    public static class CommonFriendReduce extends Reducer<Text, Text, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
                
                /* Extract the Followee-Follower Pair for Fixed Follower */
            
                ArrayList<String[]> FolloweeSet = new ArrayList<String[]>();
                
                for (Text t : value)
                    FolloweeSet.add(t.toString().split(" "));
                
                ArrayList<String> FolloweeSet1 = new ArrayList(Arrays.asList(FolloweeSet.get(0))); 
                ArrayList<String> FolloweeSet2 = new ArrayList(Arrays.asList(FolloweeSet.get(1)));
                
                /* Find the Common Followee */
                
                List<String> FolloweeIntersection = new ArrayList<String>();
                for (String s : FolloweeSet1) {
                    if(FolloweeSet2.contains(s))
                        FolloweeIntersection.add(s);
                }
                
                /* Calculate the Similarity */
               
                double sim = (double)FolloweeIntersection.size() / 
                        (double)(FolloweeSet1.size() + FolloweeSet2.size() - FolloweeIntersection.size());
                
                /* Formatted the Output & Generate the Check Sum */
                              
                double checksum = 0;

                StringBuffer FormmattedRecord = new StringBuffer();
                FormmattedRecord.append(key.toString());
                FormmattedRecord.append(", {");
                
                for (int i = 0; i < FolloweeIntersection.size(); i++) {

                    FormmattedRecord.append(FolloweeIntersection.get(i));
                    checksum = checksum + Double.parseDouble(FolloweeIntersection.get(i));
                    if (i != FolloweeIntersection.size() - 1)
                        FormmattedRecord.append(", ");
                    
                }

                FormmattedRecord.append("}, ");
                FormmattedRecord.append(String.valueOf(checksum));

                context.write(new Text(FormmattedRecord.toString()), new DoubleWritable(sim));
                    
        }
                
    }
    
    public static class SortMap extends Mapper<Text, DoubleWritable, DoubleWritable, Text> { 
        
	public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            
                    /* Job Chaining for Sorting the Pair by Similarity */
            
                    context.write(value, key);
                    
	}
        
    }
    
    public static class SortReduce extends Reducer<DoubleWritable, Text, Text, Text> {
        
        /* Output Only Top K Record */
        
        private int count; // Need a Class Member Variable
        private int k;
        private double current;
       
        protected void setup(Context context) throws IOException, InterruptedException {    
            
          /* Get the TopK */
          
          Configuration conf = context.getConfiguration();
          String TopK = conf.get("TopK");
          k = Integer.parseInt(TopK);
          count = 0;
          current = 0;

          
        }
                
        public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            Map<Integer,String> TempMap = new TreeMap<Integer,String>(); 
            
            for (Text t : value){
                        
                if( count < k ){                           
                    
                    String[] TempArray = t.toString().split(":");
                    TempMap.put(Integer.parseInt(TempArray[0]),TempArray[1]);
                    
                    if(Double.compare(current,key.get()) != 0){
                        count ++;
                        current = key.get();
                    }
                    
                }
                else{
                    break;}
                
            }
            
            ArrayList<Integer> Key = new ArrayList<Integer>(TempMap.keySet());
            for (Integer d : Key) 
                context.write(new Text(String.valueOf(d) + ":"), new Text(TempMap.get(d)));
            
        }
        
    }
    
    private static class DoubleWritableDecreasingComparator extends DoubleWritable.Comparator {  

        public int compare(WritableComparable a, WritableComparable b) { 

            return -super.compare(a, b);  

        }       

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  

            return -super.compare(b1, s1, l1, b2, s2, l2);  

        }  

    }       
    
    public static void main(String[] args) throws Exception {
        
        /*
        
        bin/hadoop jar cd.jar \
        CommunityDetection /small/* [Input Dir] \
        /small-output [Output Dir] \
        /small-temp-1 [Temp Dir] \
        /small-temp-2 [Temp Dir] \
        3 [K] \
	50 [NumberOfMapTasks] \
	20 [NumberOfReduceTasks] \
        
        */
        
        Configuration conf = new Configuration();
        long FileLength;
        conf.set("TopK", args[4]);
        
        Job job0 = new Job(conf, "Preprocessing");
        job0.setOutputFormatClass(SequenceFileOutputFormat.class);

        job0.setJarByClass(CommunityDetection.class);
        job0.setMapperClass(PreprocessingMap.class);
        job0.setReducerClass(PreprocessingReduce.class);

        job0.setMapOutputKeyClass(Text.class);
        job0.setMapOutputValueClass(Text.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
        
        /* Job Chaining Need SequenceFileInputFormat */
        
        FileInputFormat.addInputPath(job0, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job0, new Path(args[2])); 
        
        /* Set the Number of Map Tasks & Number of Reduce Tasks */
          
        FileSystem fs = FileSystem.get(conf);
        FileLength = fs.listStatus(new Path(args[0].replaceAll("\\*", "")))[0].getLen();
        FileInputFormat.setMaxInputSplitSize(job0,(FileLength/Integer.parseInt(args[5])));
        job0.setNumReduceTasks(Integer.parseInt(args[6]));	
        
        /* For Large Data Set 
        
        SequenceFileOutputFormat.setCompressOutput(job0, true); 
        
        */  
     
        job0.waitForCompletion(true);

        Job job1 = new Job(conf, "CommunityDetection");
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	
        job1.setJarByClass(CommunityDetection.class);
        job1.setMapperClass(CommonFriendMap.class);
        job1.setReducerClass(CommonFriendReduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        SequenceFileInputFormat.addInputPath(job1, new Path(args[2]));
        SequenceFileOutputFormat.setOutputPath(job1, new Path(args[3]));
        
        job1.setNumReduceTasks(Integer.parseInt(args[6]));
        
        /* For Large Data Set 
   
 	SequenceFileInputFormat.setMinInputSplitSize(job0,268435456);
        SequenceFileOutputFormat.setCompressOutput(job1, true);  
        
        */

        job1.waitForCompletion(true);
        
        Job job2 = new Job(conf, "SortMap");
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setSortComparatorClass(DoubleWritableDecreasingComparator.class);
        
        job2.setJarByClass(CommunityDetection.class);
        job2.setMapperClass(SortMap.class);
	job2.setReducerClass(SortReduce.class);
        
        job2.setMapOutputKeyClass(DoubleWritable.class);
	job2.setMapOutputValueClass(Text.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
        
        SequenceFileInputFormat.addInputPath(job2, new Path(args[3]));
	FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        /* Number of Reduced Tasks is 1 for Selecting the Top K */
        
        job2.setNumReduceTasks(1);        
        
        job2.waitForCompletion(true);
        
    }
}