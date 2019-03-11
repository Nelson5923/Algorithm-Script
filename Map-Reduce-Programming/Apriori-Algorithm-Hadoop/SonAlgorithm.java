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

public class SonAlgorithm {
        
    public static class FindCandidateMap extends Mapper<LongWritable, Text, Text, Text> {  
               
        public static ArrayList<String> FirstPass(double Threshold, ArrayList<String> FileCache) throws IOException{

            HashMap<String, Integer> ItemsSet = new HashMap<String, Integer>();
            int TotalUser = 0;

            /* Count the Single Items */

            for(String line : FileCache) {

                /* Generate the User Item and Count */

                TotalUser++;
                ArrayList<String> UserInterest = new ArrayList();

                String Regex = "interest?(.*?)\\|";

                Matcher InterestMatch = Pattern.compile(Regex).matcher(line);

                while(InterestMatch.find()){
                    UserInterest.add(InterestMatch.group());
                }

                for(String s : UserInterest){

                    String[] Splitted = s.replaceAll("\\|","").split(" ");

                    for(int i = 1; i < Splitted.length; i++){

                        StringBuffer Item = new StringBuffer();
                        Item.append(Splitted[0] + ":" + Splitted[i]);   
                        Integer Count = ItemsSet.get(Item.toString());
                        if(Count == null) Count = new Integer(0);
                        Count++;
                        ItemsSet.put(Item.toString(), Count);   

                    }

                }

            }

            /* Find the Frequent Single Items */

            Set<String> Keys = ItemsSet.keySet();
            ArrayList<String> CandidateSet = new ArrayList();
            for (String s : Keys) {
                double Support = (double)ItemsSet.get(s)/(double)TotalUser;
                if(Support >= Threshold) CandidateSet.add(s);
            }

            Collections.sort(CandidateSet);

            return CandidateSet;

        }
        
        public static ArrayList<String> SecondPass(ArrayList<String> CandidateSet, double Threshold, ArrayList<String> FileCache) throws IOException{

            /* Initialize the Candidate Pair */

            HashMap<String, Integer> CandidatePairSet = new HashMap<String, Integer>();

            for(int i = 0; i < CandidateSet.size() - 1; i++){
                for(int j = i + 1; j < CandidateSet.size(); j++){
                    String CandidatePair = CandidateSet.get(i) + " " + CandidateSet.get(j);
                    CandidatePairSet.put(CandidatePair, new Integer(0)); 
                }
            }

            int TotalUser = 0;
            Set<String> CandidatePairKeys = CandidatePairSet.keySet();

            /* For each Record, Count the Frequency in the Candidate Pair */

            for(String line : FileCache) {

                    /* Generate the User Items */

                    TotalUser++;
                    ArrayList<String> UserInterest  = new ArrayList();
                    ArrayList<String> UserItemSet  = new ArrayList();

                    String Regex = "interest?(.*?)\\|";
                    Matcher InterestMatch = Pattern.compile(Regex).matcher(line);

                    while(InterestMatch.find()){
                        UserInterest.add(InterestMatch.group());
                    }

                    for(String s : UserInterest){

                        String[] Splitted = s.replaceAll("\\|","").split(" ");     

                        for(int i = 1; i < Splitted.length; i++){

                            StringBuffer Item = new StringBuffer();
                            Item.append(Splitted[0] + ":" + Splitted[i]);
                            if(CandidateSet.contains(Item.toString()))
                                UserItemSet.add(Item.toString());

                        }

                    }

                    Collections.sort(UserItemSet);

                    /* Count the Frequency */

                    for(int i = 0; i < UserItemSet.size() - 1; i++){
                        for(int j = i + 1; j < UserItemSet.size(); j++){
                            String UserItemSetPair = UserItemSet.get(i)  + " " + UserItemSet.get(j);
                            Integer Count = CandidatePairSet.get(UserItemSetPair);
                            Count++;
                            CandidatePairSet.put(UserItemSetPair, Count);
                        }
                    }

            }

            /* Find the Frequent Pair */

            ArrayList<String>  FrequentPairSet = new ArrayList();

            for (String s : CandidatePairKeys) {
                double Support = (double)CandidatePairSet.get(s)/(double)TotalUser;
                if(Support >= Threshold)
                   FrequentPairSet.add(s);
            }

            return FrequentPairSet;        

        }

        ArrayList<String> FileCache;
        ArrayList<String> CandidateSet;
        ArrayList<String> FrequentPairSet;
        double Threshold;
        double s;
        double p;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            
            FileCache = new ArrayList();
            CandidateSet = new ArrayList();
            FrequentPairSet = new ArrayList();
            
            Configuration conf = context.getConfiguration();
            s = Double.parseDouble(conf.get("Threshold"));
            p = (double)1 / (double)Integer.parseInt(conf.get("Chunks"));
           
        }   
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
             
            FileCache.add(value.toString());
            
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
            Threshold = p * s;
            CandidateSet = FirstPass(Threshold, FileCache);
            FrequentPairSet = SecondPass(CandidateSet, Threshold, FileCache);

            for(String s : FrequentPairSet){
                context.write(new Text(s), new Text(""));
            }
            
        }
        
    }
    
    public static class FindCandidateReduce extends Reducer<Text, Text, Text, Text> {
        
    	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            context.write(new Text(key), new Text(""));
   
        }
        
    }
        
    public static class FindFrequentMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        String[] CandidatePairSet;
        Set<String> CandidateSet;
        HashMap<String, Integer> CandidatePairMap;
  
        protected void setup(Context context) throws IOException, InterruptedException {    
                      
            Configuration conf = context.getConfiguration();
            CandidatePairSet = conf.get("CandidatePair").split(",");
            CandidateSet = new HashSet<>();
            CandidatePairMap = new HashMap<String, Integer>();
            
            for(String s : CandidatePairSet){
                String[] CandidateSingle = s.split(" ");
                CandidateSet.add(CandidateSingle[0]);
                CandidateSet.add(CandidateSingle[1]);
                CandidatePairMap.put(s, new Integer(0)); 
            }          
            
        }              
          
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
        
            ArrayList<String> UserInterest  = new ArrayList();
            ArrayList<String> UserItemSet  = new ArrayList();
                
            String Regex = "interest?(.*?)\\|";
            Matcher InterestMatch = Pattern.compile(Regex).matcher(value.toString());

            while(InterestMatch.find()){
                    UserInterest.add(InterestMatch.group());
            }

            for(String s : UserInterest){

                String[] Splitted = s.replaceAll("\\|","").split(" ");     

                for(int i = 1; i < Splitted.length; i++){

                    StringBuffer Item = new StringBuffer();
                    Item.append(Splitted[0] + ":" + Splitted[i]);
                    if(CandidateSet.contains(Item.toString()))
                        UserItemSet.add(Item.toString());

                }
                    
            }
            
            Collections.sort(UserItemSet);
            
            for(int i = 0; i < UserItemSet.size() - 1; i++){
                for(int j = i + 1; j < UserItemSet.size(); j++){
                    
                    String UserItemSetPair = UserItemSet.get(i) + " " + UserItemSet.get(j);                  
                    Integer Count = CandidatePairMap.get(UserItemSetPair);
                    if(Count == null) continue;
                    Count++;
                    CandidatePairMap.put(UserItemSetPair, Count);
                    
                }
            }
            
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
            ArrayList<String> Key = new ArrayList<String>(CandidatePairMap.keySet());
            for (String s : Key) 
                context.write(new Text(s), new IntWritable(CandidatePairMap.get(s)));
            
        }
        
    }
    
    public static class FindFrequentReduce extends Reducer<Text, IntWritable, IntWritable, Text> {
        
        double s;
        int TotalUser;
        
        protected void setup(Context context) throws IOException, InterruptedException {    
                      
          Configuration conf = context.getConfiguration();
          TotalUser = Integer.parseInt(conf.get("TotalUser"));
          s = Double.parseDouble(conf.get("Threshold"));
          
        }        
        
    	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            
            int Count = 0;
       
            for(IntWritable d : value)
                Count = Count + d.get();
            
            double Support = (double) Count / (double) TotalUser;
            if(Support >= s){
                context.write(new IntWritable(Count), new Text(key));
            }
            
        }
        
    }
            
    public static void main(String[] args) throws Exception {
        
        /* 
     
        hadoop jar cd.jar SonAlgorithm \
        /son/* [InputDir] \
        /son-temp [TempDir] \
        /son-output [OutputDir] \
        0.4 [Threshold] \
        4 [Number of Chunks] \
        342000 [TotalUser] \
        
        */
        
        Configuration conf = new Configuration();
        conf.set("Threshold", args[3]);
        conf.set("Chunks", args[4]);
        conf.set("TotalUser", args[5]);
        
        /* Find Candidate */ 
        
        Job job1 = new Job(conf, "FindCandidate");
       
        job1.setJarByClass(SonAlgorithm.class);
        job1.setMapperClass(FindCandidateMap.class);
        job1.setReducerClass(FindCandidateReduce.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        FileSystem fs = FileSystem.get(conf);
        long FileLength = fs.listStatus(new Path(args[0].replaceAll("\\*", "")))[0].getLen();
        FileInputFormat.setMaxInputSplitSize(job1,(FileLength/Integer.parseInt(args[4])));
        job1.setNumReduceTasks(1);
        
        job1.waitForCompletion(true);

        /* Supplementary File */
        
        InputStream in = fs.open(new Path(args[1] + "/part-r-00000"));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        StringBuffer CandidatePair = new StringBuffer();
	while((line = br.readLine()) != null){
           line = line.trim();
	   CandidatePair.append(line).append(",");
	}
       
        conf.set("CandidatePair", CandidatePair.toString());
        
        /* Find Frequent Item */
        
        Job job2 = new Job(conf, "FindFrequent");       

        job2.setJarByClass(SonAlgorithm.class);
        job2.setMapperClass(FindFrequentMap.class);
        job2.setReducerClass(FindFrequentReduce.class);
        
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true); 
 
  }
    
}
