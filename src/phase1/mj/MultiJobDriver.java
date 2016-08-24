package phase1.mj;

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

import lib.FieldDefinition;

public class MultiJobDriver {

	public static void main(String[] args) throws Exception {
		// 開始時間
		long startTime = System.currentTimeMillis();

		// 發送工作
		Job[] jobPool = new Job[FieldDefinition.getTypeLength()];
		for (int i = 0; i < FieldDefinition.getTypeLength(); i++) {
			jobPool[i] = makeJob(i);
			jobPool[i].submit();
		}

		// 檢查是否全部工作都完成
		for (int i = 0; i < jobPool.length; i++) {
			if (!jobPool[i].isComplete()) {
				i = 0;
			}
		}

		// 把檔案移出暫存區
		for (String cubeName : FieldDefinition.CUBE_NAME) {
			moveOut(cubeName);
		}

		// 計算時間
		long totTime = System.currentTimeMillis() - startTime;
		FileWriter fw = new FileWriter("MultiJob.log");
		fw.write("Total:" + totTime);
		fw.flush();
		fw.close();
	}

	private static Job makeJob(int cubeIndex) throws IOException {
		Configuration conf = new Configuration();
		int[] fields = FieldDefinition.getCubeType(cubeIndex);
		// 指定要處理的欄位
		String[] cubeName = new String[fields.length];
		IntWritable[] types = new IntWritable[fields.length];
		for (int i = 0; i < fields.length; i++) {
			types[i] = new IntWritable(fields[i]);
			cubeName[i] = FieldDefinition.getFieldName(fields[i]);
		}
		DefaultStringifier.storeArray(conf, types, "cube_type");

		Path input = new Path("all.txt");
		Path output = new Path("MultiJobOut", "temp" + String.join("_", cubeName));

		// 建立工作
		Job job = Job.getInstance(conf,
				"MultiJobCube>" + String.join("_", cubeName));
		job.setJarByClass(MultiJobDriver.class);
		job.setMapperClass(MultiJobMap.class);
		job.setReducerClass(MultiJobReduce.class);

		// 定義輸出類型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 建立輸入輸出目錄
		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); // 才不會產生原本開頭是part的多餘檔案

		return job;
	}

	/**
	 * 將資料移出並刪除暫存區
	 */
	static void moveOut(String fileName) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 移動輸出檔案
		Path oldName = new Path(Paths.get("MultiJobOut", "temp" + fileName, fileName + "-r-00000")
				.toString());
		Path newName = new Path("MultiJobOut", fileName);
		fs.rename(oldName, newName);
		fs.delete(new Path("MultiJobOut", "temp" + fileName), true);
	}
}
