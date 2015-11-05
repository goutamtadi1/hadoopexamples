package com.tadi.mapreduce.chapter7;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressSample {

	public static void main(String args[]) {
		CompressionOutputStream out;
		InputStream in;
		try {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(BZip2Codec.class, conf);

			in = new BufferedInputStream(new FileInputStream(args[0]));
			out = codec.createOutputStream(fs.create(new Path(args[1])));

			IOUtils.copyBytes(in, out, 4096, false);
			out.close();
			in.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

}
