package com.tadi.mapreduce.chapter5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileCreate {

	public static void main(String args[]) throws IOException {
		
		Configuration conf = new Configuration();			// Configuration --> contains all configurations of cluster.
		//conf.set("fs.defaultFS","hdfs://127.0.0.1:8020/");	// namenode address
		FileSystem fs = FileSystem.get(conf);				// FileSystem --> represents HDFS.
		fs.mkdirs(new Path(args[0]));
		Path f = new Path(args[1]);							// Path --> similar to File Path and 
		fs.create(f);	
				
	}

}
