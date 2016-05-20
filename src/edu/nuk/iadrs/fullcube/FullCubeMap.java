package edu.nuk.iadrs.fullcube;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FullCubeMap extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// Input value like "0#1#1234#1234    10"
		String[] rawData = key.toString().split("#");
		int count = Integer.parseInt(value.toString());

		String[] data = new String[] { "", "", "", "" };
		for (int i = 0; i < rawData.length; i++) {
			data[i] = rawData[i];
		}

		// get time value from file name.
		String fileName = ((FileSplit) context.getInputSplit()).getPath()
				.getName();
		String month = fileName.split("-")[0];

		String[] fieldKey = { data[0], data[1], month };
		String[] fieldValue = { data[2], data[3], Integer.toString(count) };

		context.write(new Text(String.join("#", fieldKey)),
				new Text(String.join("#", fieldValue)));
	}
}
