package org.mdp.learn.hadoop.average_grade_revisited;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.junit.Test;

/**
 * quick and dirty test for the custom raw comparator
 */
public class CourseAndStudentKeyComparatorTest {

  CourseAndStudentKeyComparator comparator = new CourseAndStudentKeyComparator();

  @Test
  public void test() throws IOException {

    byte[] Algorithms_Albert = serialize(new CourseAndStudentWritable("Algorithms", "Albert"));
    byte[] Algorithms_Zeus = serialize(new CourseAndStudentWritable("Algorithms", "Zeus"));
    byte[] Database_Marco = serialize(new CourseAndStudentWritable("Database", "Marco"));
    byte[] Algorithms_Albert2 = serialize(new CourseAndStudentWritable("Algorithms", "Albert"));


    assertThat(compare(Algorithms_Albert, Algorithms_Albert2)).isZero();
    
    assertThat(compare(Algorithms_Albert, Algorithms_Zeus)).isGreaterThan(0);
    assertThat(compare(Algorithms_Zeus, Algorithms_Albert)).isLessThan(0);
    
    assertThat(compare(Algorithms_Albert, Database_Marco)).isLessThan(0);
    assertThat(compare(Database_Marco, Algorithms_Albert)).isGreaterThan(0);
  }

  private int compare(byte[] one, byte[] two) {
    return comparator.compare(one, 0, one.length, two, 0, two.length);
  }

  public static byte[] serialize(Writable writable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    writable.write(dataOut);
    dataOut.close();
    return out.toByteArray();
  }

}
