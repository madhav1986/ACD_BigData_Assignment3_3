import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerCompanyWise extends Reducer<Text, IntWritable, Text, IntWritable>
{

	@Override
	protected void reduce(Text keys, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException 
	{
		int count = 0; // initializing variable
		
		for(IntWritable values:value) // logic for summing up the values for each key
		{
			count=values.get()+count;
		}
		
		context.write(keys, new IntWritable(count)); // writing the key,value to context
	}
}
