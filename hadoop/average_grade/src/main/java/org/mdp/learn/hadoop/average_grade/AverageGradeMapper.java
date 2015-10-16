package org.mdp.learn.hadoop.average_grade;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageGradeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text word = new Text();
	private IntWritable age = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split(";");
		word.set(fields[0]);
		age.set(Integer.parseInt(fields[2].trim()));
		context.write(word, age);
	}

}