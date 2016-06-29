package mra.smj;

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

public class SmallestMultiJobDriver {

	/**
	 * Argument is input and output cuboid's name. example: args[0] is AGE_SEX
	 * args[1...] is AGE this program will build AGE cuboid from AGE_SEX.
	 * 
	 * @param args
	 * @throws IllegalArgumentException
	 */
	public static void main(String[] args) throws Exception {
		String outName = new String();
		Configuration conf = new Configuration();
		Text[] outputCuboids = null;
		if (args.length >= 2) // coubiud bane to intwritable array.
		{
			outputCuboids = new Text[args.length - 1];
			for (int i = 1; i < args.length; i++) {
				outputCuboids[i - 1] = new Text(args[i]);
				outName += args[i] + ";";
			}
			DefaultStringifier.storeArray(conf, outputCuboids, "output_cube");
			DefaultStringifier.store(conf, new Text(args[0]), "input_cube");
		} else {
			throw new IllegalArgumentException(
					"Argument need input_cuboid and output_cuboid.\nhadoop jar SMJ.jar in1 out1 [out2...]");
		}

		Job job = Job.getInstance(conf, "Smallest:" + args[0] + "->" + outName);
		job.setJarByClass(mra.smj.SmallestMultiJobDriver.class);
		job.setMapperClass(mra.smj.SmallestMultiJobMap.class);
		job.setReducerClass(mra.smj.SmallestMultiJobReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path inPath = null;
		// 第一個方體要從all.txt中做輸入，其他方體都是從前一輪完成的方體決定。
		if (args[0].equals("all.txt")) {
			inPath = new Path("all.txt");
		} else {
			inPath = new Path("SmallestOut", args[0]);
		}
		FileInputFormat.setInputPaths(job, inPath);
		Path outPath = new Path("SmallestOut", "temp" + args[0]);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); // 才不會產生原本開頭是part的多餘檔案

		if (!job.waitForCompletion(true))
			return;

		// 移動輸出檔案
		for (Text outFileName : outputCuboids) {
			String fileName = outFileName.toString();
			Path oldName = new Path(Paths.get("SmallestOut", "temp" + args[0], fileName + "-r-00000").toString());
			Path newName = new Path("SmallestOut", fileName);
			fs.rename(oldName, newName);
		}
	}

}
