package org.mdp.learn.hadoop.average_grade_revisited;

import static org.mdp.learn.hadoop.average_grade_revisited.AverageGradeJobConstants.COURSE_INDEX;
import static org.mdp.learn.hadoop.average_grade_revisited.AverageGradeJobConstants.GRADE_INDEX;
import static org.mdp.learn.hadoop.average_grade_revisited.AverageGradeJobConstants.INPUT_FIELD_SEPARATOR;
import static org.mdp.learn.hadoop.average_grade_revisited.AverageGradeJobConstants.STUDENT_INDEX;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageGradeMapper extends Mapper<LongWritable, Text, CourseAndStudentWritable, Sum> {

  private final Map<CourseAndStudentWritable, Sum> map = new HashMap<>();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(INPUT_FIELD_SEPARATOR);
    int grade = Integer.parseInt(fields[GRADE_INDEX].trim());
    CourseAndStudentWritable mapKey = new CourseAndStudentWritable(fields[COURSE_INDEX].trim(),
        fields[STUDENT_INDEX].trim());

    if (map.containsKey(mapKey)) {
      map.get(mapKey).add(grade);
    } else {
      map.put(mapKey, new Sum(grade));
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Iterator<Entry<CourseAndStudentWritable, Sum>> iterator = map.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<CourseAndStudentWritable, Sum> entry = iterator.next();
      context.write(entry.getKey(), entry.getValue());
    }
  }

}