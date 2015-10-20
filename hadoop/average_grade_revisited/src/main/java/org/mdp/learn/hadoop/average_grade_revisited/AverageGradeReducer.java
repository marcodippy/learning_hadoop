package org.mdp.learn.hadoop.average_grade_revisited;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageGradeReducer
    extends Reducer<CourseAndStudentWritable, Sum, CourseAndStudentWritable, FloatWritable> {

  @Override
  protected void reduce(CourseAndStudentWritable key, Iterable<Sum> sums, Context context)
      throws IOException, InterruptedException {
    int totSum = 0, count = 0;

    for (Sum sum : sums) {
      totSum += sum.getSum().get();
      count += sum.getCount().get();
    }

    context.write(key, new FloatWritable((float) totSum / count));
  }

}