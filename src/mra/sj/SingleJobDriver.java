package mra.sj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleJobDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SingleJobCube");
		job.setJarByClass(mra.sj.SingleJobDriver.class);
		job.setMapperClass(mra.sj.SingleJobMap.class);

		job.setReducerClass(mra.sj.SingleJobReduce.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		Path input = new Path("all.txt");
		Path output = new Path("SingleJobCubeOut");
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true))
			return;
	}

}
