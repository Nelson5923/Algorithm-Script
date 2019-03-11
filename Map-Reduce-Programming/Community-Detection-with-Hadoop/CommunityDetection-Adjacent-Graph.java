import java.io.*;
import java.util.*;
import java.lang.Runtime;

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

    public static class CommonFriendMap extends Mapper<LongWritable, Text, Text, Text> { 
        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                    
            String line = value.toString();
            String[] lineArray = line.split(":");
            String Follower = lineArray[0];
            String Followee = lineArray[1].trim();
            
            String[] tempArray = new String[2];
            tempArray[0] = Follower;
            
            StringTokenizer itr = new StringTokenizer(Followee);
            
            while (itr.hasMoreTokens()) {
                
                tempArray[0] = Follower;
                tempArray[1] = itr.nextToken();
                Arrays.sort(tempArray);
                
                context.write(new Text(tempArray[0] + ": " + tempArray[1]), new Text(Followee));
                
            }
	}
    }
    
    public static class CommonFriendReduce extends Reducer<Text, Text, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
                
                ArrayList<String[]> tmpText = new ArrayList<String[]>();
                
                for (Text text : value){
                        tmpText.add(text.toString().split(" "));
                }
 
                List<String> FolloweeUnion = new LinkedList<String>();
                
                for(String Followee1 : tmpText.get(0)){
                        for(String Followee2 : tmpText.get(1)){
                                if(Followee1.equals(Followee2)){
                                        FolloweeUnion.add(Followee1);
                                }
                        }
                }
                
                double sim = (double)FolloweeUnion.size() / (double)(tmpText.get(0).length + tmpText.get(1).length - FolloweeUnion.size());
                int checksum = 0;
                
                StringBuffer sb = new StringBuffer();
                sb.append(key.toString());
                sb.append(", {");
                
                for(int i = 0; i < FolloweeUnion.size(); i++){
                        
                        sb.append(FolloweeUnion.get(i));
                        checksum = checksum + Integer.parseInt(FolloweeUnion.get(i));
                        
                        if(i != FolloweeUnion.size() - 1)
                                sb.append(", ");
                        
                }
                
                sb.append("}, ");
                sb.append(String.valueOf(checksum));
                
                context.write(new Text(sb.toString()), new DoubleWritable(sim));
                
        }
    }
    
    public static class SortMap extends Mapper<Text, DoubleWritable, DoubleWritable, Text> { 
        
	public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            
                    /* Job Chaining for Sorting the Pair by Similarity */
            
                    context.write(value, key);
                    
	}
        
    }
    
    public static class SortReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        
        /* Output Only Top K Record */
        
        int count = 0; // Need a Class Member Variable
                
	public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            /* Get the TopK */
            
            Configuration conf = context.getConfiguration();
            String TopK = conf.get("TopK");
            int k = Integer.parseInt(TopK);
            
            //System.out.printf("%d\n",k);
            
            for (Text text : value){
                        
                        if( count < k ){
                            
                            context.write(text,key);
                            count++;
                            
                        }else{
                            
                            break;
                        
                        }
            }
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
        /small-temp [Temp Dir] \
        5 [Data Size] \
        3 [K]
        
        */
        
        Configuration conf = new Configuration();
        conf.set("DataSize", args[3]);
        conf.set("TopK", args[4]);
        
        Job job1 = new Job(conf, "CommunityDetection");
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setJarByClass(CommunityDetection.class);
        job1.setMapperClass(CommonFriendMap.class);
        job1.setReducerClass(CommonFriendReduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.waitForCompletion(true);
        
        /* Job Chaining Need SequenceFileInputFormat */
        
        Job job2 = new Job(conf, "SortMap");
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setSortComparatorClass(DoubleWritableDecreasingComparator.class);
        
        /* Number of Reduced Tasks is 1 for Selecting the Top K */
        
        job2.setNumReduceTasks(1);
        
        job2.setJarByClass(CommunityDetection.class);
        job2.setMapperClass(SortMap.class);
	job2.setReducerClass(SortReduce.class);
        
        job2.setMapOutputKeyClass(DoubleWritable.class);
	job2.setMapOutputValueClass(Text.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(DoubleWritable.class);
        
        SequenceFileInputFormat.addInputPath(job2, new Path(args[2]));
	FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        job2.waitForCompletion(true);
        
  }

}