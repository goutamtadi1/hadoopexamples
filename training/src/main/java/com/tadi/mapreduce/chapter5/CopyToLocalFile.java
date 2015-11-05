package com.tadi.mapreduce.chapter5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyToLocalFile {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();			// Configuration --> contains all configurations of cluster.
		FileSystem fs = FileSystem.get(conf);	
		fs.copyToLocalFile(new Path(args[0]), new Path(args[1]));
	}

}
