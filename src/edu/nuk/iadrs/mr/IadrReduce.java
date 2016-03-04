package edu.nuk.iadrs.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.nuk.iadrs.data.PassKey;

public class IadrReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text _key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		// process values
		int sum = 0;
		for (IntWritable val : values) {
			sum += 1;
		}
		
		// process key
		
		context.write(_key, new IntWritable(sum));
	}

}
