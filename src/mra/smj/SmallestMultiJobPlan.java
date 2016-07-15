package mra.smj;

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

public class SmallestMultiJobPlan {
	public static void main(String[] args) throws Exception {
		// 開始計時
		long startTime = System.currentTimeMillis();
		// LV.0
		String inL0 = "all.txt";
		String[] outL0 = new String[] { "AGE_SEX_DRUG_PT" };
		Job topJob = makeJob(inL0, outL0);
		if (!topJob.waitForCompletion(true))
			return;
		moveOut(inL0, outL0);
		removeTemp(inL0);

		// LV.1
		String inL1 = "AGE_SEX_DRUG_PT";
//		String inL1 = "all.txt";
		String[] outL1 = new String[] { "AGE_SEX_DRUG", "AGE_SEX_PT",
				"AGE_DRUG_PT", "SEX_DRUG_PT" };
		Job jobL1 = makeJob(inL1, outL1);
		if (!jobL1.waitForCompletion(true))
			return;
		moveOut(inL1, outL1);
		removeTemp(inL1);

		// LV.2
		Thread[] plan = new Thread[] { new Plan0(), new Plan1(), new Plan2() };
		for (int i = 0; i < plan.length; i++) {
			plan[i].start();
		}
		for (int i = 0; i < plan.length; i++) {
			plan[i].join();
		}

		// 計算時間
		long totTime = System.currentTimeMillis() - startTime;
		FileWriter fw = new FileWriter("SmallestJob.log");
		fw.write("Total:" + totTime);
		fw.flush();
		fw.close();
	}

	static Job makeJob(String input, String[] output) throws IOException {
		Configuration conf = new Configuration();
		Text[] outCubeName = new Text[output.length];
		for (int i = 0; i < output.length; i++) {
			outCubeName[i] = new Text(output[i]);
		}
		DefaultStringifier.storeArray(conf, outCubeName, "output_cube");
		DefaultStringifier.store(conf, new Text(input), "input_cube");
		String outName = String.join("#", output);

		Job job = Job.getInstance(conf, "Smallest:" + input + "->" + outName);
		job.setJarByClass(mra.smj.SmallestMultiJobPlan.class);
		job.setMapperClass(mra.smj.SmallestMultiJobMap.class);
		job.setReducerClass(mra.smj.SmallestMultiJobReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path inPath = null;
		// 第一個方體要從all.txt中做輸入，其他方體都是從前一輪完成的方體決定。
		if (input.equals("all.txt")) {
			inPath = new Path("all.txt");
		} else {
			inPath = new Path("SmallestOut", input);
		}
		FileInputFormat.setInputPaths(job, inPath);
		Path outPath = new Path("SmallestOut", "temp" + input);
		FileOutputFormat.setOutputPath(job, outPath);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); // 才不會產生原本開頭是part的多餘檔案
		return job;
	}

	/**
	 * 將資料移出暫存區
	 */
	static void moveOut(String parentDir, String[] childFiles)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 移動輸出檔案
		for (String outFileName : childFiles) {
			Path oldName = new Path(Paths.get("SmallestOut",
					"temp" + parentDir, outFileName + "-r-00000").toString());
			Path newName = new Path("SmallestOut", outFileName);
			fs.rename(oldName, newName);
		}
	}

	/**
	 * 刪除暫存檔案
	 */
	static void removeTemp(String parentDir) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("SmallestOut", "temp" + parentDir), true);
	}
}

/**
 * ASD -> AS, AD, SD -> A, S, D -> ALL
 */
class Plan0 extends Thread {
	public void run() {
		// LV.2
		String inJ1 = "AGE_SEX_DRUG";
		String[] outJ1 = new String[] { "AGE_SEX", "AGE_DRUG", "SEX_DRUG" };
		Job job1;
		try {
			job1 = SmallestMultiJobPlan.makeJob(inJ1, outJ1);
			if (!job1.waitForCompletion(true))
				return;
			SmallestMultiJobPlan.moveOut(inJ1, outJ1);
			SmallestMultiJobPlan.removeTemp(inJ1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
		// LV.3
		// 這個工作後續不用等，所以先做
		String inJ22 = "SEX_DRUG";
		String[] outJ22 = new String[] { "DRUG" };
		Job job22;
		try {
			job22 = SmallestMultiJobPlan.makeJob(inJ22, outJ22);
			job22.submit();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}

		String inJ21 = "AGE_SEX";
		String[] outJ21 = new String[] { "AGE", "SEX" };
		Job job21;
		try {
			job21 = SmallestMultiJobPlan.makeJob(inJ21, outJ21);
			if (!job21.waitForCompletion(true))
				return;
			SmallestMultiJobPlan.moveOut(inJ21, outJ21);
			SmallestMultiJobPlan.removeTemp(inJ21);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}

		// LV.4
		String inJ3 = "SEX";
		String[] outJ3 = new String[] { "ALL" };
		Job job3;
		try {
			job3 = SmallestMultiJobPlan.makeJob(inJ3, outJ3);
			if (!job3.waitForCompletion(true))
				return;
			SmallestMultiJobPlan.moveOut(inJ3, outJ3);
			SmallestMultiJobPlan.removeTemp(inJ3);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
		
		
		// move DRUG cuboid
		try {
			SmallestMultiJobPlan.moveOut(inJ22, outJ22);
			SmallestMultiJobPlan.removeTemp(inJ22);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

/**
 * ASP -> AP, SP -> P
 */
class Plan1 extends Thread {
	public void run() {
		// LV.2
		String inJ1 = "AGE_SEX_PT";
		String[] outJ1 = new String[] { "AGE_PT", "SEX_PT" };
		Job job1;
		try {
			job1 = SmallestMultiJobPlan.makeJob(inJ1, outJ1);
			if (!job1.waitForCompletion(true))
				return;
			SmallestMultiJobPlan.moveOut(inJ1, outJ1);
			SmallestMultiJobPlan.removeTemp(inJ1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
		// LV.3
		String inJ2 = "SEX_PT";
		String[] outJ2 = new String[] { "PT" };
		Job job2;
		try {
			job2 = SmallestMultiJobPlan.makeJob(inJ2, outJ2);
			if (!job2.waitForCompletion(true))
				return;
			SmallestMultiJobPlan.moveOut(inJ2, outJ2);
			SmallestMultiJobPlan.removeTemp(inJ2);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}

/**
 * SDP -> DP
 */
class Plan2 extends Thread {
	public void run() {
		// LV.2
		String inJ1 = "SEX_DRUG_PT";
//		String inJ1 = "all.txt";
		String[] outJ1 = new String[] { "DRUG_PT" };
		Job job1;
		try {
			job1 = SmallestMultiJobPlan.makeJob(inJ1, outJ1);
			if (!job1.waitForCompletion(true))
				return;
			SmallestMultiJobPlan.moveOut(inJ1, outJ1);
			SmallestMultiJobPlan.removeTemp(inJ1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}