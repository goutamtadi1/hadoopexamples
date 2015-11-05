package com.tadi.mapreduce.chapter10;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileWriteDemo {

	private static final String[] DATA = { "A", "B", "C", "D" };

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		IntWritable key = new IntWritable();
		Text value = new Text();

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path(args[0]);
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
			for (int i = 0; i < 200; i++) {
				key.set(200 - i);
				value.set(DATA[i % DATA.length]);
				writer.append(key, value);
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			writer.close();
		}

	}

}
