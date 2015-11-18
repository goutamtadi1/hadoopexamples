package com.tadi.mapreduce.chapter13;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MultipleInputFormats {
	
	enum COUNTER{
		good_records,bad_records
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", ";");
		//conf.set("mapreduce.input.lineinputformat.linespermap", "3");
		conf.set("mapred.textoutputformat.separator", "-");
		
		Job job = new Job(conf);
		
		job.setMapperClass(MultipleKeyValueTextInputMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
