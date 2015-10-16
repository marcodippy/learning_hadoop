package org.mdp.learn.hadoop.average_grade_with_combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageGradeDriverWithCombiner extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(AverageGradeDriverWithCombiner.class);
		job.setJobName("AverageGradeDriverWithCombiner");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(AverageGradeMapperWithCombiner.class);
		job.setCombinerClass(AverageGradeCombiner.class);
		job.setReducerClass(AverageGradeReducerWithCombiner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntPair.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new AverageGradeDriverWithCombiner(), args);
		System.exit(exitCode);
	}
}
