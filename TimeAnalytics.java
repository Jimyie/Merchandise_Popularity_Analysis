import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class TimeAnalytics {
	public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
 
 private Text word = new Text();
   
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  String line=value.toString();
  String[] values = line.split(",");
  Date time=null;
 	 SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm");
 	try {
		time=sdf.parse(values[0]);
	} catch (ParseException e) {
		// TODO Auto-generated catch block		
		e.printStackTrace();
	}
				    String class1=values[1];
		    	    String class2=values[2];				   	 
		    	    String class3=values[3];		   	 
		    	    String class4=values[4];			   	  
		    	    String brand=values[5];				   
		    	    String commentvolume=values[6];    
				   	 if(commentvolume.equals(" INC"))
				   		  commentvolume=values[7];			    		  
		    	    String collectvolume=values[7];
				   	  if(values[6].equals(" INC"))
			    		 collectvolume=values[8];
		    	   Date stardardbottom= null;
		    	   String stardardbottomstring="2015-09-30 23:59";
		    	   try {
					stardardbottom=sdf.parse(stardardbottomstring);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	   Date stardardpeak= null;
		    	   String stardardpeakstring="2016-01-01 00:00";
		    	   try {
					stardardpeak= sdf.parse(stardardpeakstring);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				   	  if(time.after(stardardbottom)&&time.before(stardardpeak))
		    	    { 	
		    	    	System.out.println(time);
		    	    	word.set(brand);
		    	    	int volume=Integer.parseInt(commentvolume)+Integer.parseInt(collectvolume);		    	        
		    	        context.write(word, new IntWritable(volume));		    	        
		    	    } 			  
   
 }
}

public static class IntSumReducer 
    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();
 public void reduce(Text key, Iterable<IntWritable> values, Context context
                    ) throws IOException, InterruptedException {
   int sum = 0;
   int count=0;
   for (IntWritable val : values) {
     sum += val.get();
     count++;
   }
   int average=sum/count;
   result.set(average);
   context.write(key, result);
 }
}

public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 if (otherArgs.length != 2) {
   System.err.println("Usage: wordcount <in> <out>");
   System.exit(2);
 }
 Job job = new Job(conf, "word count");
 job.setJarByClass(TimeAnalytics.class);
 job.setMapperClass(TokenizerMapper.class);
 job.setCombinerClass(IntSumReducer.class);
 job.setReducerClass(IntSumReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(IntWritable.class);
 FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/tmp/smzdm.csv"));
 FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/tmp/out"));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
