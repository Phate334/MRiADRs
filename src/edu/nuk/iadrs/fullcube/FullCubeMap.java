package edu.nuk.iadrs.fullcube;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FullCubeMap extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] raw = value.toString().split("\t");
		String[] rawData = raw[0].split("#");
		int count = Integer.parseInt(raw[1]);
		
		String[] data = new String[]{"","","",""};
		for(int i=0;i<rawData.length;i++)
		{
			data[i] = rawData[i];
		}
		
		String[] fieldKey = {data[0],data[1]};
		String[] fieldValue = {data[2],data[3],Integer.toString(count)};
		
		context.write(new Text(String.join("#", fieldKey)), new Text(String.join("#", fieldValue)));
	}
	
}
