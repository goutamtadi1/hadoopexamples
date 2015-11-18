package com.tadi.mapreduce.chapter13;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// Key --> LongWritable, Value --> Text
// Every line of a file is record.
// key --> offset of line , value --> actual data
public class MultipleTextInputMapper extends Mapper<LongWritable,Text,LongWritable,Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println(key.toString()+"\t"+ value.toString());
		String[] split = value.toString().split(";");
		System.out.println(split[0]+"\t"+split[1]);
		context.write(key, value);
	}

}
