package org.mdp.learn.hadoop.average_grade;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageGradeReducer extends Reducer<Text, IntPair, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntPair> value, Reducer<Text, IntPair, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int sum = 0, count = 0, avg = 0;

		for (IntPair intPair : value) {
			sum += intPair.getSum();
			count += intPair.getCount();
		}
		
		avg = sum / count;
		
		context.write(key, new IntWritable(avg));
	}
 
}