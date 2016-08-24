package phase1.smj;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import lib.FieldData;
import lib.FieldDefinition;

public class SmallestMultiJobMap extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private String inputCube;
	private IntWritable[][] outCubesType;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		inputCube = DefaultStringifier.load(conf, "input_cube", Text.class)
				.toString();

		Text[] outCuboid = DefaultStringifier.loadArray(conf, "output_cube",
				Text.class);
		outCubesType = new IntWritable[outCuboid.length][];
		// 處理輸入的多組方體名稱
		for (int i = 0; i < outCuboid.length; i++) {
			String[] types = outCuboid[i].toString().split("_");
			outCubesType[i] = new IntWritable[types.length];
			// 處理每一組方體名稱中的欄位
			for (int j = 0; j < types.length; j++) {
				int fieldType = FieldDefinition.getFieldIndex(types[j]);
				if (fieldType == -1) {
					throw new IllegalArgumentException(
							"ERROR: bad field name: " + outCuboid[i]);
				}
				outCubesType[i][j] = new IntWritable(fieldType);
			}
		}

	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		FieldData data = null;
		IntWritable count = null;
		// 判斷輸入資料類型
		try {
			if (inputCube.equals("all.txt")) {
				data = new FieldData(value.toString());
				count = new IntWritable(1);
			} else {
				String[] temp = value.toString().split("\t");
				data = new FieldData(temp[0]);
				count = new IntWritable(Integer.parseInt(temp[1]));
			}
			// 輸出資料對
			for (int i = 0; i < outCubesType.length; i++) {
				// 藥物和不良反應的組合
				for (String rowData : data.getKey(outCubesType[i])) {
					context.write(new Text(rowData), count);
				}
			}
		} catch (ParseException e) {
			// pass
		}
	}

}
