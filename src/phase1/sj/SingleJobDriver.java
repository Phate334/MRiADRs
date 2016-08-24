package phase1.sj;

import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleJobDriver {

	public static void main(String[] args) throws Exception {
		// 開始時間
		long startTime = System.currentTimeMillis();
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SingleJob");
		job.setJarByClass(phase1.sj.SingleJobDriver.class);
		job.setMapperClass(phase1.sj.SingleJobMap.class);

		job.setReducerClass(phase1.sj.SingleJobReduce.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		Path input = new Path("all.txt");
		Path output = new Path("SingleJobOut");
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true))
			return;
		
		// 計算時間
		long totTime = System.currentTimeMillis() - startTime;
		FileWriter fw = new FileWriter("SingleJob.log");
		fw.write("Total:" + totTime);
		fw.flush();
		fw.close();
	}

}
