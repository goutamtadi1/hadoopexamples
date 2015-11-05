import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			// CA --> [25,30,40]
			Iterator<IntWritable> itr = values.iterator();
			ArrayList<Integer> meanList = new ArrayList<Integer>();
			while(itr.hasNext()){
				meanList.add(itr.next().get());
			}
			int mean = calculateMean(meanList);
			context.write(new Text(key), new IntWritable(mean));

		}

		private int calculateMean(ArrayList<Integer> meanList){
			
			Iterator<Integer> itr = meanList.iterator();
			int sum =0;
			while(itr.hasNext()){
				sum+=itr.next();
			}
			int mean = ((sum)/(meanList.size()));
			return mean;
		}
	

}
