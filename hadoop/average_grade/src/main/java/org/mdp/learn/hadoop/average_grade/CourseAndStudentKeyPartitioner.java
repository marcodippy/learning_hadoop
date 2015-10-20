package org.mdp.learn.hadoop.average_grade;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CourseAndStudentKeyPartitioner extends Partitioner<CourseAndStudentWritable, IntWritable> {

  @Override
  public int getPartition(CourseAndStudentWritable mapKey, IntWritable grade, int numReduceTasks) {
    return (mapKey.getCourse().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }

}
