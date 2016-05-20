package edu.nuk.iadrs.valuea;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.nuk.iadrs.data.FieldDefinition;

public class SingleJobReduce extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> out;

	@Override
	protected void cleanup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		out.close();
		super.cleanup(context);
	}

	@Override
	protected void setup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		out = new MultipleOutputs<>(context);
		super.setup(context);
	}

	public void reduce(Text _key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		// process key
		out.write(_key, new IntWritable(sum),
				FieldDefinition.getCubeTypeName(_key.toString()));
	}

}
