package edu.nuk.iadrs;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OneCubeMap extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			Field f = new Field(value.toString());
			context.write(new Text("1"), new Text("1"));
		}
		catch(ParseException e){}
	}
}
