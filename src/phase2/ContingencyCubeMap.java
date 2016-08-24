package phase2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import lib.FieldDefinition;

public class ContingencyCubeMap extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// Input value like "0#1#1234#1234#0410    10"
		String[] rawData = key.toString().split("#");
		int count = Integer.parseInt(value.toString());

		String[] data = new String[4];
		for (int i = 0; i < 4; i++) {
			data[i] = rawData[i];
		}

		// get time value.
		String month = rawData[FieldDefinition.DATE_MONTH];

		String[] fieldKey = { data[0], data[1], month };
		String[] fieldValue = { data[2], data[3], Integer.toString(count) };

		context.write(new Text(String.join("#", fieldKey)),
				new Text(String.join("#", fieldValue)));
	}
}
