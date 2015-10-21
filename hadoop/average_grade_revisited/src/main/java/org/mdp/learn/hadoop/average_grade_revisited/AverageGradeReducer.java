package org.mdp.learn.hadoop.average_grade_revisited;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageGradeReducer extends Reducer<CourseAndStudentWritable, Sum, CourseAndStudentWritable, FloatWritable> {
  private final Map<Text, Integer> coursesCounter  = new HashMap<>();
  private final Map<Text, Integer> studentsCounter = new HashMap<>();

  @Override
  protected void reduce(CourseAndStudentWritable key, Iterable<Sum> sums, Context context) throws IOException, InterruptedException {
    int totSum = 0, count = 0;

    for (Sum sum : sums) {
      totSum += sum.getSum().get();
      count += sum.getCount().get();
    }

    context.write(key, new FloatWritable((float) totSum / count));
    updateIntermediateCounters(key);
  }

  private void updateIntermediateCounters(CourseAndStudentWritable key) {
    coursesCounter.put(key.getCourse(), coursesCounter.getOrDefault(key.getCourse(), 0) + 1);
    studentsCounter.put(key.getStudent(), studentsCounter.getOrDefault(key.getStudent(), 0) + 1);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    coursesCounter.forEach((k, v) -> context.getCounter(MyCounters.COURSES).increment(1));
    studentsCounter.forEach((k, v) -> context.getCounter(MyCounters.STUDENTS).increment(1));

    // dynamic counters
    coursesCounter.forEach((k, v) -> context.getCounter("STUDENTS_PER_COURSE", k.toString()).increment(v));
    studentsCounter.forEach((k, v) -> context.getCounter("COURSES_PER_STUDENT", k.toString()).increment(v));
  }

}