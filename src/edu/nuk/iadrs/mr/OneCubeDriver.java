package edu.nuk.iadrs.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.nuk.iadrs.data.FieldDefinition;


public class OneCubeDriver {

	public static void main(String[] args) throws Exception {
		Path input = new Path("all.txt");
		Path output = new Path("out");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//指定要處理的欄位
		IntWritable[] types = new IntWritable[FieldDefinition.CUBE_TYPE[5].length];
		for(int i=0;i<FieldDefinition.CUBE_TYPE[5].length;i++)
		{
			types[i] = new IntWritable(FieldDefinition.CUBE_TYPE[5][i]);
		}
		DefaultStringifier.storeArray(conf, types, "cube_type");
		
		//建立工作
		Job job = Job.getInstance(conf, "OneCube");
		job.setJarByClass(edu.nuk.iadrs.mr.OneCubeDriver.class);
		job.setMapperClass(edu.nuk.iadrs.mr.OneCubeMap.class);

		job.setReducerClass(edu.nuk.iadrs.mr.IadrReduce.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		fs.delete(output, true);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true))
			return;
	}

}
