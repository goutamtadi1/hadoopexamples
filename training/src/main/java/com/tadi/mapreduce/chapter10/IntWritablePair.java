package com.tadi.mapreduce.chapter10;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntWritablePair implements Writable {

	private IntWritable intW1, intW2;

	public IntWritable getIntW1() {
		return intW1;
	}

	public void setIntW1(IntWritable intW1) {
		this.intW1 = intW1;
	}

	public IntWritable getIntW2() {
		return intW2;
	}

	public void setIntW2(IntWritable intW2) {
		this.intW2 = intW2;
	}

	public IntWritablePair() {
		if(intW1==null){
			intW1 = new IntWritable();
		}
		
		if(intW2==null){
			intW2 = new IntWritable();
		}		
	}

	public IntWritablePair(int intW1, int intW2) {
		this.intW1 = new IntWritable(intW1);
		this.intW2 = new IntWritable(intW2);
	}
	
	public IntWritablePair(IntWritable intW1, IntWritable intW2) {
		this.intW1 = intW1;
		this.intW2 = intW2;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		intW1.write(out);
		intW2.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		intW1.readFields(in);
		intW2.readFields(in);
	}

}
