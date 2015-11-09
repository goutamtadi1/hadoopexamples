package com.tadi.mapreduce.Multiple;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MultipleInputDemo {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", "-");
		Job job = new Job(conf);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[2]),true);

		//job.setMapperClass(MultipleOutputMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MultipleInputMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), KeyValueTextInputFormat.class, MultipleInputMapper2.class);

		job.waitForCompletion(true);
	}

}
