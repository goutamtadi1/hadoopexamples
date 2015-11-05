import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// <InputKey, InputValue, OutputKey, OutputValue>
public class MachineAssignmentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			System.out.println(key.toString()); //123
			System.out.println(value.toString()); //hi this is sample data
			
			StringTokenizer str = new StringTokenizer(value.toString()," "); // [hi,this,is,sample,data]
			
			while(str.hasMoreTokens()){
				context.write(new Text(str.nextToken()), new IntWritable(1)); // hi --> 1, this --> 1
			}
	
		}
	

}
