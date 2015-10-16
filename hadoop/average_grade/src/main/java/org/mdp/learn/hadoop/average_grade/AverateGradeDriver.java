package org.mdp.learn.hadoop.average_grade;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;

public class AverateGradeDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf());
		job.setJobName("Average Grade");
		job.setJarByClass(AverateGradeDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(AverageGradeMapper.class);
		job.setReducerClass(AverageGradeReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		HdfsUtils.deleteIfExists(getConf(), outputFilePath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		AverateGradeDriver wordcountDriver = new AverateGradeDriver();
		int res = ToolRunner.run(wordcountDriver, args);
		System.exit(res);
	}
}
