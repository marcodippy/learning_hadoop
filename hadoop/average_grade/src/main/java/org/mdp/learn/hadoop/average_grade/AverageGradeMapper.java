package org.mdp.learn.hadoop.average_grade;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import static org.mdp.learn.hadoop.average_grade.AverageGradeJobConstants.*;

public class AverageGradeMapper extends Mapper<LongWritable, Text, CourseAndStudentWritable, IntWritable> {
  private final CourseAndStudentWritable mapKey                = new CourseAndStudentWritable();
  private final IntWritable              grade                 = new IntWritable();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(INPUT_FIELD_SEPARATOR);

    mapKey.set(fields[COURSE_INDEX].trim(), fields[STUDENT_INDEX].trim());
    grade.set(Integer.parseInt(fields[GRADE_INDEX].trim()));

    context.write(mapKey, grade);
  }

}