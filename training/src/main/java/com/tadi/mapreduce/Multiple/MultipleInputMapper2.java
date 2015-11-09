package com.tadi.mapreduce.Multiple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleInputMapper2 extends Mapper<Text, Text, LongWritable, Text> {

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(new LongWritable(), new Text(key.toString()+";"+value.toString())); //0	CA;24
	}

}
