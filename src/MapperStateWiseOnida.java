import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperStateWiseOnida extends Mapper<LongWritable, Text, Text, IntWritable>
{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{
		// TODO Auto-generated method stub
		String lineArray[]=value.toString().split("\\|"); // split on the basis of | delimiter
		
		String companyName = lineArray[0]; // assigning value to companyName
		String state=lineArray[3]; // assigning value to stateName
		
		if(companyName.equals("Onida")) // Condition to check if company name is "Onida"
		{
			context.write(new Text(state), new IntWritable(1)); // writing the key,value to context
		}
	}
}
