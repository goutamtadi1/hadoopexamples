package com.tadi.mapreduce.chapter13;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Key --> Text, Value --> Text
// Every line of a file is record.
// key --> , value --> 
public class MultipleNLineInputMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println(key.toString() + "\t" + value.toString());

		context.write(key, value);
	}

}