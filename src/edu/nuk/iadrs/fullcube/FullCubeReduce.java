package edu.nuk.iadrs.fullcube;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FullCubeReduce extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> out;

	@Override
	protected void cleanup(
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		out.close();
		super.cleanup(context);
	}

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		out = new MultipleOutputs<>(context);
		super.setup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// 取出 Key
		String[] keys = key.toString().split("#");
		String month = keys[2];

		Map<String[], Integer> mainValue = new HashMap<String[], Integer>();
		Map<String, Integer> drugMap = new HashMap<String, Integer>();
		Map<String, Integer> ptMap = new HashMap<String, Integer>();
		int total = 0;

		// 分開Value
		for (Text data : values) {
			String[] row = data.toString().split("#"); // drug, PT, count
			int count = Integer.parseInt(row[2]);
			if (!row[0].equals("") && !row[1].equals("")) {
				mainValue.put(new String[] { row[0], row[1] }, count);
			} else if (!row[0].equals("") && row[1].equals("")) // 只有 drug
			{
				drugMap.put(row[0], count);
			} else if (row[0].equals("") && !row[1].equals("")) // 只有PT
			{
				ptMap.put(row[1], count);
			} else if (row[0].equals("") && row[1].equals("")) {
				total = count;
			}
		}
		
		for(String[] dpValue: mainValue.keySet())
		{
			int aValue = mainValue.get(dpValue);
			int bValue = drugMap.get(dpValue[0]) - aValue;
			int cValue = ptMap.get(dpValue[1]) - aValue;
			int dValue = total - aValue - bValue - cValue;
			String[] fullCount = {
					String.valueOf(aValue),
					String.valueOf(bValue),
					String.valueOf(cValue),
					String.valueOf(dValue),
			};
			
			// output
			String[] outKey = {
					keys[0],
					keys[1],
					dpValue[0],
					dpValue[1]
			};
			out.write(new Text(String.join("#", outKey)), new Text(String.join("#", fullCount)), month);
		}
	}
}
