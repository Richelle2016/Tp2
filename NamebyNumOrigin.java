package com.namecount.hadoop.NamebyOrigin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NamebyNumOrigin {
	
	 	public static class NamebyOriginMapper extends Mapper<Object, Text, Text, IntWritable>{

	 			private final static IntWritable one = new IntWritable(1);
	 			private Text word = new Text();

	 			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	 				String line = value.toString();
	 	            String[] str=line.split(";");
	 	            String[] s=str[2].split(",");
	 	            for(int x=0;x<s.length;x++)
	 	            {
	 	            	word.set(Integer.toString(s.length));
	 	                context.write(word, one);
	 	            }
	 			}
	 	}
		
		public static class NamebyOriginReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		  private IntWritable result = new IntWritable();
		
		  public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		    int sum = 0;
		    for (IntWritable val : values) {
		      sum += val.get();
		    }
		    result.set(sum);
		    context.write(key, result);
		  }
		} 	

	public static void main(String[] args) throws IOException,InterruptedException, ClassNotFoundException{
		
		Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
 
        // Create configuration
        Configuration conf = new Configuration(true);
 
        // Create job
        Job job = new Job(conf, "NameCount");
        job.setJarByClass(NamebyOriginDriver.class);
 
        // Setup MapReduce
        job.setMapperClass(NamebyOriginMapper.class);
        job.setReducerClass(NamebyOriginReducer.class);
       // job.setNumReduceTasks(1);
 
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        //job.setInputFormatClass(TextInputFormat.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
       // job.setOutputFormatClass(TextOutputFormat.class);
 
        // Delete output if exists
        /*FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);*/
 
        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
 
	}

}
