import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//CA;34
		
		String record = value.toString();
		StringTokenizer strToken = new StringTokenizer(record,";");
		String city = strToken.nextToken();
		int temperature = Integer.parseInt(strToken.nextToken());
		context.write(new Text(city), new IntWritable(temperature));
	}

}
