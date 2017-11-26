import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerStateWiseOnida extends Reducer<Text, IntWritable, Text, IntWritable>
{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException 
	{
		// TODO Auto-generated method stub
		
		int count=0; // initializing variable
		
		for(IntWritable value:values) // logic for summing up the values for each key
		{
			count=value.get()+count;
		}
		
		context.write(key, new IntWritable(count)); // writing the key,value to context
	}

}
