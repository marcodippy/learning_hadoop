package org.mdp.learn.hadoop.average_grade;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ClassAndStudentWritable implements WritableComparable<ClassAndStudentWritable> {
	private final Text course, student;

	public ClassAndStudentWritable() {
		this.course = new Text();
		this.student = new Text();
	}

	public ClassAndStudentWritable(Text course, Text student) {
		this.course = course;
		this.student = student;
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
	public int compareTo(ClassAndStudentWritable o) {
		int ret = course.compareTo(o.course);
		return (ret != 0) ? ret : student.compareTo(o.student);
	}

	@Override
	public String toString() {
		return course + " -- " + student;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ClassAndStudentWritable) {
			ClassAndStudentWritable casw = (ClassAndStudentWritable) o;
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
