import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCountGmc {
        
 public static class MapGmc extends Mapper<LongWritable, Text, Text, IntWritable> 
 {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Map<String, Integer> map;
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        Map<String, Integer> map = getMap();
        while (tokenizer.hasMoreTokens()) 
        {
        	String token =tokenizer.nextToken();
        	if(map.containsKey(token)) 
        	  {
        		int total = map.get(token) + 1;
        		map.put(token, total);   
        	  } 
        	else 
        	{    
        		map.put(token, 1);   
        	}                    	
        	//word.set(tokenizer.nextToken());
            //context.write(word, one);
        }
        //--
    }    
    //--
    protected void cleanup(Context context)  throws IOException, InterruptedException 
    {  
    	Map<String, Integer> map = getMap();  
    	Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();  
    	while(it.hasNext()) 
    	{   
    		Map.Entry<String, Integer> entry = it.next();   
    		String sKey = entry.getKey();   
    		int total = entry.getValue().intValue();   
    		context.write(new Text(sKey), new IntWritable(total));  
    	} 
    }    
    //--
    public Map<String, Integer> getMap() 
      {  
    	if(null == map)    
    		map = new HashMap<String, Integer>();  
    	return map; 
      }
    
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
 {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "WordCount");
    job.setJarByClass(WordCountGmc.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(MapGmc.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    //FileInputFormat.addInputPath(job, new Path("genesis.txt"));
    //FileOutputFormat.setOutputPath(job, new Path("output"));

        
    job.waitForCompletion(true);
 }
        
}