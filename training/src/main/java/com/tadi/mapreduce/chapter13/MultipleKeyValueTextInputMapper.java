package com.tadi.mapreduce.chapter13;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.tadi.mapreduce.chapter13.MultipleInputFormats.COUNTER;

// Key --> Text, Value --> Text
// Every line of a file is record.
// key --> , value --> 
public class MultipleKeyValueTextInputMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println(key.toString() + "\t" + value.toString());
		if(value.toString().contains("999")){
			context.getCounter(COUNTER.bad_records).increment(1);
		}
		else{
			context.getCounter(COUNTER.good_records).increment(1);
		}
		context.write(key, value);
	}

}