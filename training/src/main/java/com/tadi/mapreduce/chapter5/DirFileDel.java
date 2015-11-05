package com.tadi.mapreduce.chapter5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DirFileDel {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();			// Configuration --> contains all configurations of cluster.
		FileSystem fs = FileSystem.get(conf);	
		fs.delete(new Path(args[0]));
		fs.delete(new Path(args[1]), true); 
	}

}
