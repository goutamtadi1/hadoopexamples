import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.tadi.mapreduce.chapter10.IntWritablePair;

public class MeanCombiner extends Reducer<Text, IntWritablePair, Text, IntWritablePair> {

	

		@Override
		protected void reduce(Text key, Iterable<IntWritablePair> values, Context context)
				throws IOException, InterruptedException {
			
			// CA --> [25,30,40]
			Iterator<IntWritablePair> itr = values.iterator();
			ArrayList<Integer> meanList = new ArrayList<Integer>();
			while(itr.hasNext()){
				meanList.add(itr.next().getIntW1().get());
			}
			
			context.write(new Text(key), sumAndCount(meanList));

		}

		/*private int calculateMean(ArrayList<Integer> meanList){
			
			
			int mean = ((sum)/(meanList.size()));
			return mean;
		}*/
	
		private IntWritablePair sumAndCount(ArrayList<Integer> meanList){
			Iterator<Integer> itr = meanList.iterator();
			int sum =0;
			while(itr.hasNext()){
				sum+=itr.next();
			}
			IntWritablePair tp = new IntWritablePair();
			tp.setIntW1(new IntWritable(sum));
			tp.setIntW2(new IntWritable(meanList.size()));
			return tp;
		}
}
