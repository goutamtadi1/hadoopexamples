package org.tadi.hadoop.training;

import org.junit.Test;

import com.tadi.mapreduce.chapter10.IntWritablePair;

public class TestIntWritablePair {

	@Test
	public void testIntWritablePair(){
		IntWritablePair tp = new IntWritablePair();
		System.out.println(tp.getIntW1().get());
		System.out.println(tp.getIntW2().get());
	}
}
