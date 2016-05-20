package edu.nuk.iadrs.valuea;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.nuk.iadrs.data.FieldData;
import edu.nuk.iadrs.data.FieldDefinition;

public class SingleJobMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		for (int i = 0; i < FieldDefinition.getTypeLength(); i++) {  // cube 類型
			int[] fields = FieldDefinition.getCubeType(i);
			IntWritable[] indexs = getIntWritableArray(fields);
			try {
				FieldData data = new FieldData(ivalue.toString());
				for (String rowData : data.getKey(indexs)) {  // 藥物和不良反應的組合
					context.write(new Text(rowData), new IntWritable(1));
				}
			} catch (ParseException e) {
			}

		}
	}

	private IntWritable[] getIntWritableArray(int[] intArray) {
		IntWritable[] intWritableArray = new IntWritable[intArray.length];
		for (int i = 0; i < intArray.length; i++) {
			intWritableArray[i] = new IntWritable(intArray[i]);
		}
		return intWritableArray;
	}

}
