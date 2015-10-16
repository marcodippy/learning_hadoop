package org.mdp.learn.hadoop.average_grade_with_combiner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageGradeMapperWithCombiner extends Mapper<LongWritable, Text, Text, IntPair> {
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntPair>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split(";");
		word.set(fields[0]);
		IntPair intpair = new IntPair();
		intpair.set(1, Integer.parseInt(fields[2].trim()));
		context.write(word, intpair);
	}

}