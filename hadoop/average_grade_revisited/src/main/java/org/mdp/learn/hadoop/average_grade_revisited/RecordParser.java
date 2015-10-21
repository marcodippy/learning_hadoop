package org.mdp.learn.hadoop.average_grade_revisited;

import static org.mdp.learn.hadoop.average_grade_revisited.AverageGradeJobConstants.INPUT_FIELD_SEPARATOR;

import static org.mdp.learn.hadoop.average_grade_revisited.AverageGradeJobConstants.COURSE_INDEX;
import static org.mdp.learn.hadoop.average_grade_revisited.AverageGradeJobConstants.GRADE_INDEX;
import static org.mdp.learn.hadoop.average_grade_revisited.AverageGradeJobConstants.STUDENT_INDEX;

import org.apache.hadoop.io.Text;

/**
 * poor implementation of a record parser
 */
public class RecordParser {
  private String[] fields;
  private String   course, student, grade;
  private boolean  wellFormed  = false;
  private boolean  validRecord = false;

  public void parse(Text record) {
    fields = record.toString().split(INPUT_FIELD_SEPARATOR);

    this.wellFormed = validateFormat();
    this.validRecord = wellFormed && validateRecord();

    if (wellFormed && validRecord) {
      this.course = fields[COURSE_INDEX].trim();
      this.student = fields[STUDENT_INDEX].trim();
      this.grade = fields[GRADE_INDEX].trim();
    }
  }

  private boolean validateFormat() {
    if (fields.length < 3)
      return false;

    return (!fields[COURSE_INDEX].trim().isEmpty() && !fields[STUDENT_INDEX].trim().isEmpty() && !fields[GRADE_INDEX].trim().isEmpty());
  }

  private boolean validateRecord() {
    return isValidGrade();
  }

  public String getCourse() {
    return course;
  }

  public String getStudent() {
    return student;
  }

  public Integer getGrade() {
    return Integer.parseInt(grade);
  }

  public boolean isWellFormed() {
    return wellFormed;
  }

  public boolean isValidGrade() {
    try {
      int grade = Integer.parseInt(fields[GRADE_INDEX].trim());
      return (60 <= grade && grade <=100);
    } catch (NumberFormatException nfe) {
      return false;
    }
  }

  public boolean isValidRecord() {
    return validRecord;
  }
}
