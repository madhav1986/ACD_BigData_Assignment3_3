// import packages
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.*;

// Mapper class 
public class MapperTask extends Mapper<LongWritable, Text, Text, NullWritable>
{
	@Override
	// overriding map method
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException 
	{

		String[] lineArray = value.toString().split("\\|"); // split on the basis of | delimiter

		String companyName=lineArray[0]; // assigning value to companyName
		String productName=lineArray[1]; // assigning value to productName

		Text output ; // creating output variable of type Text

		//System.out.println(companyName + " " + productName);
		
			if(!(companyName.equals("NA")||productName.equals("NA"))) // check whether companyName or productName is "NA"
			{
				output = value; // assigning the value to output
				context.write(output,NullWritable.get()); // writing the key,value to context
			}

	}
}
