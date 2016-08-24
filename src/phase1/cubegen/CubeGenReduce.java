package phase1.cubegen;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import lib.FieldData;
import lib.FieldDefinition;

public class CubeGenReduce extends Reducer<Text, Text, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> out;
	private String[][] outTypeName = null;
	private IntWritable[][] cubeType = null;
	private HashMap<String, Integer>  outMap = null;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Text[] outAttributes = DefaultStringifier.loadArray(conf, "output",
				Text.class);
		outTypeName = new String[outAttributes.length][];
		cubeType = new IntWritable[outAttributes.length][];
		for (int i = 0; i < outAttributes.length; i++) {
			outTypeName[i] = outAttributes[i].toString().split("_");
			cubeType[i] = new IntWritable[outTypeName[i].length];
			for (int j = 0; j < outTypeName[i].length; j++) {
				cubeType[i][j] = new IntWritable(
						FieldDefinition.getFieldIndex(outTypeName[i][j]));
			}
		}
		
		outMap = new HashMap<String, Integer>();
		out = new MultipleOutputs<>(context);
	}

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// 計算reduce收到的VALUE
		for (Text val : values) {
			FieldData data = null;
			try {
				data = new FieldData(val.toString());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			for (int typeIndex = 0; typeIndex < outTypeName.length; typeIndex++) {
				String redKey = data.getKey(cubeType[typeIndex]).get(0);
				if (outMap.containsKey(redKey)) {
					outMap.put(redKey, outMap.get(redKey) + 1);
				} else {
					outMap.put(redKey, 0);
				}
			}
		}
		// 輸出加總值
		for (String element : outMap.keySet()) {
			out.write(new Text(element), new IntWritable(outMap.get(element)),
					  FieldDefinition.getCubeTypeName(element));
		}
		outMap.clear();
	}

	@Override
	protected void cleanup(
			Reducer<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		out.close();
		super.cleanup(context);
	}
}
