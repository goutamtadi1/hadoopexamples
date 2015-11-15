package com.tadi.mapreduce.chapter10;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ADT implements Writable {
	// id,name,salary,dept

	private LongWritable id;
	private Text name;
	private IntWritable salary;
	private Text dept;

	public LongWritable getId() {
		return id;
	}

	public void setId(LongWritable id) {
		this.id = id;
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public IntWritable getSalary() {
		return salary;
	}

	public void setSalary(IntWritable salary) {
		this.salary = salary;
	}

	public Text getDept() {
		return dept;
	}

	public void setDept(Text dept) {
		this.dept = dept;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		name.write(out);
		salary.write(out);
		dept.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		name.readFields(in);
		salary.readFields(in);
		dept.readFields(in);
	}

}
