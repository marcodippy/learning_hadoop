package org.mdp.learn.hadoop.average_grade;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CourseAndStudentKeyGroupingComparator extends WritableComparator {

  protected CourseAndStudentKeyGroupingComparator() {
    super(CourseAndStudentWritable.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    CourseAndStudentWritable casw1 = (CourseAndStudentWritable) a;
    CourseAndStudentWritable casw2 = (CourseAndStudentWritable) b;

    int cmp = casw1.getCourse().compareTo(casw2.getCourse());
    return (cmp != 0) ? cmp : (casw1.getStudent().compareTo(casw2.getStudent()));
  }

}
