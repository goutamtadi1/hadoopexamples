package org.tadi.hadoop.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hello world!
 *
 */

// Driver
// Mapper
// Reducer
public class WordCount extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
           ToolRunner.run(new WordCount(), args);
    }

	@Override
	public int run(String[] args) throws Exception {
		
		
		
		 Configuration conf = this.getConf();
	       // contains all the configurations for HDFS, MR
		 Job job = new Job(conf);

		 //Delete the output folder for every run.
		 FileSystem fs = FileSystem.get(conf);
		 fs.delete(new Path(args[1]), true);
		 
		 job.setMapperClass(WordCountMapper.class);
		 job.setNumReduceTasks(0);
		 
		 //job.setReducerClass(cls);
		 
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);
		 
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(IntWritable.class);
		 
		 //job.setOutputKeyClass(theClass);
		 //job.setOutputValueClass(theClass);
		 
		 job.waitForCompletion(true);
		 
		 return 0;
	}
}
