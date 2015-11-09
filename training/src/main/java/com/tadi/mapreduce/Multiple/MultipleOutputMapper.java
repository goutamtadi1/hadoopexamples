package com.tadi.mapreduce.Multiple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleOutputMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> mos;

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String data = value.toString();
		String[] elements = data.split(";");
		try {
			if (elements[0].isEmpty() || elements[1] == null) {
				throw new Exception();
			}
			else{
				mos.write("success", NullWritable.get(), value);
				}
		} catch (Exception e) {
			mos.write("error", NullWritable.get(), value);
		}
		
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}

}
