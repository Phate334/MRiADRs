package edu.nuk.iadrs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneCubeDriver {

	public static void main(String[] args) throws Exception {
		Path input = new Path("all.txt");
		Path output = new Path("out");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Job job = Job.getInstance(conf, "OneCube");
		job.setJarByClass(edu.nuk.iadrs.OneCubeDriver.class);
		job.setMapperClass(edu.nuk.iadrs.OneCubeMap.class);

		job.setReducerClass(edu.nuk.iadrs.IadrReduce.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		fs.delete(output, true);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true))
			return;
	}

}
