package org.mdp.learn.hadoop.average_grade_revisited;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class CourseAndStudentKeyComparator extends WritableComparator {

  private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

  protected CourseAndStudentKeyComparator() {
    super(CourseAndStudentWritable.class, true);
  }
  
  //TODO what happens if I override both of these methods ?
//  @SuppressWarnings("rawtypes")
//  @Override
//  public int compare(WritableComparable a, WritableComparable b) {
//    CourseAndStudentWritable casw1 = (CourseAndStudentWritable) a;
//    CourseAndStudentWritable casw2 = (CourseAndStudentWritable) b;
//
//    int cmp = casw1.getCourse().compareTo(casw2.getCourse());
//    return (cmp != 0) ? cmp : -1 * (casw1.getStudent().compareTo(casw2.getStudent()));
//  }

  // TODO understand this code
  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    try {
      int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
      int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
      int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
      if (cmp != 0) {
        return cmp;
      }
      return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  // TODO test this stuff
  static {
    WritableComparator.define(CourseAndStudentWritable.class, new CourseAndStudentKeyComparator());
  }
}
