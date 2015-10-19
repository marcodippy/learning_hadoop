package org.mdp.learn.hadoop.average_grade;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageGradeReducer extends Reducer<CourseAndStudentWritable, IntWritable, CourseAndStudentWritable, FloatWritable> {

  @Override
  protected void reduce(CourseAndStudentWritable key, Iterable<IntWritable> grades, Context context) throws IOException, InterruptedException {
    int sum = 0, count = 0;

    for (IntWritable grade : grades) {
      sum += grade.get();
      count++;
    }

    context.write(key, new FloatWritable(sum / count));
  }

}