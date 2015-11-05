import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MachineAssignmentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private Iterator<IntWritable> iterator;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		iterator = values.iterator();
		int sum = 0;
		while(iterator.hasNext()){
			sum += iterator.next().get();
		}
		context.write(key, new IntWritable(sum));
	}

}
