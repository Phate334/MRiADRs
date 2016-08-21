package phase2;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.nuk.iadrs.data.FieldDefinition;

public class ContingencyCubeDriver {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		// 開始時間
		long startTime = System.currentTimeMillis();

		// 發送工作
		Job[] jobPool = new Job[FieldDefinition.getTypeLength()];
		for (int i = 0; i < FieldDefinition.FULL_CUBE_TYPE.length; i++) {
			jobPool[i] = makeJob(FieldDefinition.FULL_CUBE_TYPE[i]);
			if(!jobPool[i].waitForCompletion(true))
				return;
			moveOut(FieldDefinition.FULL_CUBE_TYPE[i][0]);
		}
		// 計算時間
		long totTime = System.currentTimeMillis() - startTime;
		FileWriter fw = new FileWriter("3C.log");
		fw.write("Total:" + totTime);
		fw.flush();
		fw.close();

	}

	public static Job makeJob(String[] inputCubes) throws IOException {
		Configuration conf = new Configuration();
		conf.set(inputCubes[0], "cube_type");
		Path output = new Path("FullCubeOutFromMJ", "temp" + inputCubes[0]);

		Job job = Job.getInstance(conf, "FullCube_" + inputCubes[0]);
		job.setJarByClass(ContingencyCubeDriver.class);

		job.setMapperClass(ContingencyCubeMap.class);
		job.setReducerClass(ContingencyCubeReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		conf.set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				"\t");
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);
		Path[] inPaths = new Path[inputCubes.length];
		for (int i = 0; i < inputCubes.length; i++) {
			inPaths[i] = new Path("MultiJobOut", inputCubes[i]);
		}
		FileInputFormat.setInputPaths(job, inPaths);
		FileOutputFormat.setOutputPath(job, output);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		return job;
	}

	/**
	 * 將資料移出並刪除暫存區
	 */
	static void moveOut(String cuboidName) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 移動輸出檔案
		Path oldName = new Path(Paths.get("FullCubeOutFromMJ",
				"temp" + cuboidName, "part-r-00000").toString());
		Path newName = new Path("FullCubeOutFromMJ", cuboidName);
		fs.rename(oldName, newName);
		fs.delete(new Path("FullCubeOutFromMJ", "temp" + cuboidName), true);
	}
}
