package edu.nuk.iadrs.fullcube;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FullCubeDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path input = new Path("out\\AGE");
		Path output = new Path("FullCubeOut");
		
		Job job = Job.getInstance(conf, "FullCube");
		job.setJarByClass(FullCubeDriver.class);
		
		job.setMapperClass(FullCubeMap.class);
		job.setReducerClass(FullCubeReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		if (!job.waitForCompletion(true))
			return;
	}

}
