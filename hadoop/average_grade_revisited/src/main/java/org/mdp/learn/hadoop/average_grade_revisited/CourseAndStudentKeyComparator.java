package org.mdp.learn.hadoop.average_grade_revisited;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class CourseAndStudentKeyComparator extends WritableComparator {

  private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

  protected CourseAndStudentKeyComparator() {
    super(CourseAndStudentWritable.class);
  }

  @Override
  public int compare(byte[] bytes_1, int start_1, int length_1, byte[] bytes_2, int start_2, int length_2) {
    try {
      int courseVInt_length_1 = WritableUtils.decodeVIntSize(bytes_1[start_1]);
      int courseVInt_length_2 = WritableUtils.decodeVIntSize(bytes_2[start_2]);

      int courseContentLength_1 = readVInt(bytes_1, start_1);
      int courseContentLength_2 = readVInt(bytes_2, start_2);

      int courseObjLength_1 = courseVInt_length_1 + courseContentLength_1;
      int courseObjLength_2 = courseVInt_length_2 + courseContentLength_2;

      int cmp = TEXT_COMPARATOR.compare(bytes_1, start_1, courseObjLength_1, bytes_2, start_2, courseObjLength_2);

      if (cmp != 0) {
        return cmp;
      }

      int studentStart_1 = start_1 + courseObjLength_1;
      int studentStart_2 = start_2 + courseObjLength_2;

      int studentObjLength1 = length_1 - courseObjLength_1;
      int studentObjLength2 = length_2 - courseObjLength_2;

      return -1 * TEXT_COMPARATOR.compare(bytes_1, studentStart_1, studentObjLength1, bytes_2, studentStart_2, studentObjLength2);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  static {
    WritableComparator.define(CourseAndStudentWritable.class, new CourseAndStudentKeyComparator());
  }
}
