package edu.nuk.iadrs.mr;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.nuk.iadrs.data.FieldData;

public class OneCubeMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		
		ObjectWritable obj = DefaultStringifier.load(conf, "cube_type", ObjectWritable.class);
		int[] type = (int[]) obj;
		
		try{
			FieldData data = new FieldData(value.toString());
			context.write(data.getKey(type), new IntWritable(1));
		}
		catch(ParseException e){}
		
		
	}
}
