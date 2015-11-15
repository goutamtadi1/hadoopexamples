package com.tadi.mapreduce.chapter5;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class WritetoHDFS {

public static void main(String args[]) throws IOException{
		
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS","hdfs://127.0.0.1:8020/"); // namenode address set
		FileSystem fs = FileSystem.get(conf);
		
		
		InputStream in = null;
		OutputStream out=null;
		try{
			in=new BufferedInputStream(new FileInputStream(args[0]));
			out= fs.create(new Path(args[1]));
			IOUtils.copyBytes(in, out,512,false);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
		finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
		
		System.out.println("Write operation Done...!");
	}

}

// hadoop fs -copyFromLocal <Path to local> <Path to HDFS dir>