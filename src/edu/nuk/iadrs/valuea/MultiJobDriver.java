package edu.nuk.iadrs.valuea;

import java.io.IOException;

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

import edu.nuk.iadrs.data.FieldDefinition;

public class MultiJobDriver {

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < FieldDefinition.getTypeLength(); i++) {
			Job singleCubeJob = makeJob(i);
			if (!singleCubeJob.waitForCompletion(true))
				return;
		}
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
		Path output = new Path("SingleCubeOut", String.join("_", cubeName));

		// 建立工作
		Job job = Job.getInstance(conf,
				"SingleCube" + String.join("_", cubeName));
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
}
