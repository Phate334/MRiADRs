package phase1.cubegen;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CubeGenDriver {

	private static int[] plan_in = { 3, 1, 2, 3, 1, 2 };
	private static String[] plan_out = {
			"AGE_SEX_DRUG_PT#AGE_SEX_DRUG#AGE_SEX#AGE#ALL",
			"AGE_SEX_PT#SEX_DRUG#SEX", "AGE_DRUG_PT#AGE_DRUG#DRUG",
			"SEX_DRUG_PT#AGE_PT#PT", "SEX_PT", "DRUG_PT" };

	public static void main(String[] args) throws Exception {
		// 開始時間
		long startTime = System.currentTimeMillis();

		// 建立工作
		Job[] jobPool = new Job[plan_in.length];
		for (int i = 0; i < plan_in.length; i++) {
			String[] cubes = plan_out[i].split("#");
			jobPool[i] = makeJob(plan_in[i], cubes);
			jobPool[i].submit();
		}
		// 檢查是否全部工作都完成
		for (int i = 0; i < jobPool.length; i++) {
			if (!jobPool[i].isComplete()) {
				i = 0;
			}
		}

		// 把檔案移出暫存區
		for (String cubeName : plan_out) {
			moveOut(cubeName);
		}

		// 計算時間
		long totTime = System.currentTimeMillis() - startTime;
		FileWriter fw = new FileWriter("CubeGen.log");
		fw.write("Total:" + totTime);
		fw.flush();
		fw.close();
	}

	private static Job makeJob(int input, String[] output) throws Exception {
		Configuration conf = new Configuration();
		Text[] temp = new Text[output.length];
		for (int i = 0; i < output.length; i++) {
			temp[i] = new Text(output[i]);
		}
		DefaultStringifier.store(conf, new IntWritable(input), "input");
		DefaultStringifier.storeArray(conf, temp, "output");

		Job job = Job.getInstance(conf, "CubeGen:" + String.join("#", output));
		job.setJarByClass(phase1.cubegen.CubeGenDriver.class);
		job.setMapperClass(phase1.cubegen.CubeGenMap.class);
		job.setReducerClass(phase1.cubegen.CubeGenReduce.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		Path inPath = new Path("all.txt");
		Path outPath = new Path("CubeGen", String.join("#", output));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); // 才不會產生原本開頭是part的多餘檔案

		// if (!job.waitForCompletion(true))
		// return null;

		// 把結果移出暫存區
		// for (String fileName : output) {
		// Path oldName = new Path(Paths.get("CubeGen", "temp",
		// fileName + "-r-00000").toString());
		// Path newName = new Path("CubeGen", fileName);
		// fs.rename(oldName, newName);
		// }
		return job;
	}

	/**
	 * 將資料移出並刪除暫存區
	 */
	static void moveOut(String jobName) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 移動輸出檔案
		for (String fileName : jobName.split("#")) {
			Path oldName = new Path(Paths.get("CubeGen", jobName,
					fileName + "-r-00000").toString());
			Path newName = new Path("CubeGen", fileName);
			fs.rename(oldName, newName);
		}
		fs.delete(new Path("CubeGen", jobName), true);
	}
}
