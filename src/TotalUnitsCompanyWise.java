// import required packages
import org.apache.hadoop.fs.Path; 
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

public class TotalUnitsCompanyWise 
{

	public static void main(String[] args) throws Exception // main method starts
	{
		// TODO Auto-generated method stub
		
		Configuration conf=new Configuration(); // create object of Configuration class
		
		@SuppressWarnings("deprecation")
		Job job=new Job(conf, "CompanyWise_TotalUnits"); // Create job
		
		//job configuration properties
		job.setJarByClass(TotalUnitsCompanyWise.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapperCompanyWise.class);
		
		//job.setNumReduceTasks(0);
		
		job.setReducerClass(ReducerCompanyWise.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	
	}

}
