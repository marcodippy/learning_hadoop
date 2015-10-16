package org.mdp.learn.hadoop.average_grade_with_combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageGradeReducerWithCombiner extends Reducer<Text, IntPair, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntPair> values, Reducer<Text, IntPair, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		int sum = 0;

		for (IntPair value : values) {
			count += value.getCount();
			sum += value.getSum();
		}

		int avg = sum / count;

		context.write(key, new IntWritable(avg));
	}

}
