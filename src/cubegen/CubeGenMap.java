package cubegen;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.nuk.iadrs.data.FieldData;
import edu.nuk.iadrs.data.FieldDefinition;

public class CubeGenMap extends Mapper<LongWritable, Text, Text, Text> {

	private int keyIndex;
	private IntWritable[] cubeType = null;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		// 要作為key的欄位
		keyIndex = DefaultStringifier.load(conf, "input", IntWritable.class).get();
		//把全部資料都做map的輸出value
		cubeType = new IntWritable[FieldDefinition.CUBE_TYPE[14].length];
		for (int i = 0; i < FieldDefinition.CUBE_TYPE[14].length; i++) {
			cubeType[i] = new IntWritable(FieldDefinition.CUBE_TYPE[14][i]);
		}
	}

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		try {
			FieldData data = new FieldData(ivalue.toString());
			for (String rowData : data.getKey(cubeType)) {
				Text key = new Text(rowData.split("#")[keyIndex]);
				context.write(key, new Text(rowData));
			}

		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
