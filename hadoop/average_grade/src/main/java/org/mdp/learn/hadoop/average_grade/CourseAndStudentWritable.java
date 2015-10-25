package org.mdp.learn.hadoop.average_grade;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CourseAndStudentWritable implements WritableComparable<CourseAndStudentWritable> {
  private final Text course, student;

  public CourseAndStudentWritable() {
    this.course = new Text();
    this.student = new Text();
  }

  public CourseAndStudentWritable(String course, String student) {
    this.course = new Text(course);
    this.student = new Text(student);
  }
  
  public void set (String course, String student) {
    this.course.set(course);
    this.student.set(student);
  }

  public Text getCourse() {
    return course;
  }

  public Text getStudent() {
    return student;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    course.readFields(input);
    student.readFields(input);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    course.write(out);
    student.write(out);
  }

  @Override
  public int compareTo(CourseAndStudentWritable o) {
    int ret = course.compareTo(o.course);
    return (ret != 0) ? ret : student.compareTo(o.student);
  }

  @Override
  public String toString() {
    return course + AverageGradeJobConstants.OUTPUT_FIELD_SEPARATOR + student;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CourseAndStudentWritable) {
      CourseAndStudentWritable casw = (CourseAndStudentWritable) o;
      return course.equals(casw.course) && student.equals(casw.student);
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((course == null) ? 0 : course.hashCode());
    result = prime * result + ((student == null) ? 0 : student.hashCode());
    return result;
  }

}
