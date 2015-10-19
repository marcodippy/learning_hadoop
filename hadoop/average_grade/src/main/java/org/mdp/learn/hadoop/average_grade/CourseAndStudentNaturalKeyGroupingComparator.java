package org.mdp.learn.hadoop.average_grade;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Without this class, rows related to the same course but different student may
 * go to different reducers. We need to rely only on the natural key (course).
 * This code is executed on the Reducer 
 */
public class CourseAndStudentNaturalKeyGroupingComparator extends WritableComparator {

  protected CourseAndStudentNaturalKeyGroupingComparator() {
    super(CourseAndStudentWritable.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    CourseAndStudentWritable casw1 = (CourseAndStudentWritable) a;
    CourseAndStudentWritable casw2 = (CourseAndStudentWritable) b;
    return casw1.getCourse().compareTo(casw2.getCourse());
  }

}
