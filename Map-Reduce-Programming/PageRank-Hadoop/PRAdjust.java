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

public class PRAdjust {  

    public static class PRAdjustMap extends Mapper<LongWritable, Text, LongWritable, Text> {          

        double m;        
        double a;
        double n;

        public void setup(Context context) throws IOException, InterruptedException {    
                      
            Configuration conf = context.getConfiguration();
            a = Double.parseDouble(conf.get("alpha"));      
            m = Double.parseDouble(conf.get("MissingPageRank"));
            n = Double.parseDouble(conf.get("TotalUser"));
            
        } 
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            
            StringTokenizer stk = new StringTokenizer(value.toString());
            PRNodeWritable Node = new PRNodeWritable();
            
            if(stk.hasMoreElements())
                Node.setNodeID(Integer.parseInt(stk.nextToken()));
            if(stk.hasMoreElements())
                Node.setPageRank(Double.parseDouble(stk.nextToken()));
            while(stk.hasMoreElements()) {
                Node.addList(Integer.parseInt(stk.nextToken()));
            }
            
            double p = Node.getPageRank();
            double AdjustPR = a * (1/n) + (1-a) * ((m/n) + p);
            Node.setPageRank(AdjustPR);
            
            context.write(new LongWritable(Node.getNodeID()), 
                    new Text(Node.getPageRank() + " " + Node.getList()));
             
        }
        
    }    

    public static class PRAdjustReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        
    	public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            
            for(Text s : value)
                context.write(key, s);
            
        }
        
    }    

    public static void PRAdjustDriver(double alpha, double TotalUser, 
            double TotalPageRank, String inDir, String outDir) throws Exception{
        
        /* hadoop jar [.jar file] PageRank [alpha] [threshold] [iteration] [infile] [outdir] */
        
        Configuration conf = new Configuration();
        double MissingPageRank = 1 - TotalPageRank / 1000000000;
        conf.set("MissingPageRank", String.valueOf(MissingPageRank));
        conf.set("alpha", String.valueOf(alpha));
        conf.set("TotalUser", String.valueOf(TotalUser));
        
        Job job1 = new Job(conf, "PRAdjust");
       
        job1.setJarByClass(PRAdjust.class);
        job1.setMapperClass(PRAdjustMap.class);
        job1.setReducerClass(PRAdjustReduce.class);
        
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(inDir));
        FileOutputFormat.setOutputPath(job1, new Path(outDir));
    
        job1.waitForCompletion(true);

    }    
    
}
