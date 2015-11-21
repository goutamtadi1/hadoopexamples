package org.tadi.hadoop.training;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//0	baba baba black sheep
//baba,baba,black,sheep

//Output

//baba 1
//baba 1
//black 1
//sheep 1

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final IntWritable ONE =  new IntWritable(1);

	//key --> offset
	//value --> line data.
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//breakdown line into words.
		String[] words = value.toString().split(" ");
		
		//assign value 1 to each and every word.
		for(int i =0; i<words.length;i++){
			context.write(new Text(words[i]), ONE);
		}
	}

}
