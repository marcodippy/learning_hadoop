package org.mdp.learn.hadoop.average_grade_with_combiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageGradeCombiner extends Reducer<Text, IntPair, Text, IntPair> {

	@Override
	protected void reduce(Text key, Iterable<IntPair> values, Reducer<Text, IntPair, Text, IntPair>.Context context)
			throws IOException, InterruptedException {
		int agesSum = 0;
		int count = 0;

		for (IntPair intPair : values) {
			agesSum += intPair.getSum();
			count += intPair.getCount();
		}

		IntPair intpair = new IntPair();
		intpair.set(count, agesSum);

		context.write(key, intpair);
	}

}