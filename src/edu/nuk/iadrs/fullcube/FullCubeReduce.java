package edu.nuk.iadrs.fullcube;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FullCubeReduce extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for(Text data: values)
		{
			String[] row = data.toString().split("#");
			sum += 1;
		}
		
		context.write(key, new IntWritable(sum));
	}
	
}
