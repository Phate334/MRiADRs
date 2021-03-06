package phase1.mj;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import lib.FieldData;

public class MultiJobMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		IntWritable[] fields = DefaultStringifier.loadArray(conf, "cube_type", IntWritable.class);
		
		try {
			FieldData data = new FieldData(value.toString());
			for (String rowData : data.getKey(fields)) {
				context.write(new Text(rowData), new IntWritable(1));
			}
		} catch (ParseException e) {
		}

	}
}
