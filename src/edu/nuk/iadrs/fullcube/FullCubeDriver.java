package edu.nuk.iadrs.fullcube;

import java.io.IOException;

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

public class FullCubeDriver {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		for (String[] fullCube : FieldDefinition.FULL_CUBE_TYPE) {
			Job fullCubeJob = makeJob(fullCube);
			if (!fullCubeJob.waitForCompletion(true))
				return;
		}

	}

	public static Job makeJob(String[] inputCubes) throws IOException {
		Configuration conf = new Configuration();
		conf.set(inputCubes[0], "cube_type");
		Path output = new Path("FullCubeOut", inputCubes[0]);

		Job job = Job.getInstance(conf, "FullCube_" + inputCubes[0]);
		job.setJarByClass(FullCubeDriver.class);

		job.setMapperClass(FullCubeMap.class);
		job.setReducerClass(FullCubeReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);
		Path[] inPaths = new Path[inputCubes.length];
		for (int i=0; i<inputCubes.length; i++) {
			inPaths[i] = new Path("SingleCubeOut", inputCubes[i]);
		}
		FileInputFormat.setInputPaths(job, inPaths);
		FileOutputFormat.setOutputPath(job, output);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		return job;
	}

}
