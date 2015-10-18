package org.mdp.learn.hadoop.average_grade;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageGradeMapper extends Mapper<LongWritable, Text, ClassAndStudentWritable, IntWritable> {
	private Text word = new Text();
	private IntWritable age = new IntWritable();

	private static final int CLASS_INDEX = 0;
	private static final int STUDENT_NAME_INDEX = 1;
	private static final int GRADE_INDEX = 2;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(";");

		word.set(fields[CLASS_INDEX].trim());
		age.set(Integer.parseInt(fields[GRADE_INDEX].trim()));
		context.write(word, age);
	}

}