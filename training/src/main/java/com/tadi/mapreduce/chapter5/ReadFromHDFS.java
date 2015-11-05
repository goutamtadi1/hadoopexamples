package com.tadi.mapreduce.chapter5;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ReadFromHDFS {
	
	public static void main(String args[]) throws IOException{
		
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS","hdfs://127.0.0.1:8020/");
		FileSystem fs = FileSystem.get(conf);
		
		
		InputStream in = null ;
		OutputStream fout = new FileOutputStream(args[1]); // to write into a file
		try{
			in=fs.open(new Path(args[0]));
			IOUtils.copyBytes(in,fout, 512,false);
		}
		catch(Exception e){
			
		}
		finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(fout);
		}
	}

}
