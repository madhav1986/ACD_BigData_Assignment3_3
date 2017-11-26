// import required packages
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class OnidaTotalUnitsStateWise 
{

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException // main method starts
	{
		// TODO Auto-generated method stub
		Configuration conf=new Configuration(); // create object of Configuration class
		
		@SuppressWarnings("deprecation")
		Job job=new Job(conf, "StateWise_TotalUnitsOnida"); // Create job
		
		//job configuration properties
		job.setJarByClass(OnidaTotalUnitsStateWise.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapperStateWiseOnida.class);
		
		//job.setNumReduceTasks(0);
		
		job.setReducerClass(ReducerStateWiseOnida.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		

	}

}
