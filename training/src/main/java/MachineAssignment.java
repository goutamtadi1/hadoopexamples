import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MachineAssignment extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MachineAssignment(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		
		Path outputPath = new Path(args[1]);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		Job job = new Job(conf);

		job.setMapperClass(MachineAssignmentMapper.class);
		job.setReducerClass(MachineAssignmentReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}

// Every input format sends records to map in <Key,Value> format

// TextInputFormat: Each line is a record. Key --> offset of line , Value --> line itself

	// eg: Value --> I live in Long Beach. --> Text;  Key --> offset (128) --> LongWritable
	//	   Value --> Long Beach is in California. --> offset (5000)