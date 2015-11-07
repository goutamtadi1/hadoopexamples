import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.tadi.mapreduce.chapter10.IntWritablePair;

public class MeanReducer extends Reducer<Text, IntWritablePair, Text, IntWritable> {

	

		@Override
		protected void reduce(Text key, Iterable<IntWritablePair> values, Context context)
				throws IOException, InterruptedException {
			
			// CA --> [sum/count, sum/count]
			Iterator<IntWritablePair> itr = values.iterator();
			ArrayList<Integer> meanList = new ArrayList<Integer>();
			ArrayList<Integer> countList = new ArrayList<Integer>();
			while(itr.hasNext()){
				IntWritablePair tp = itr.next();
				meanList.add(tp.getIntW1().get());
				countList.add(tp.getIntW2().get());
			}
			
			int mean = calculateMean(meanList,countList);
			context.write(new Text(key), new IntWritable(mean));

		}

		
		private int calculateMean(ArrayList<Integer> meanList, ArrayList<Integer> countList){
			Iterator<Integer> itr = meanList.iterator();
			int sum =0;
			while(itr.hasNext()){
				sum+=itr.next();
			}
			Iterator<Integer> itr1 = countList.iterator();
			int count =0;
			while(itr1.hasNext()){
				count+=itr1.next();
			}
			int mean = sum/count;
			return mean;
		}
	

}
