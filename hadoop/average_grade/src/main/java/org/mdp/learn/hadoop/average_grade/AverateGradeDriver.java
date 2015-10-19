package org.mdp.learn.hadoop.average_grade;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.commons.JobBuilder;

public class AverateGradeDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

    job.setMapperClass(AverageGradeMapper.class);
    job.setPartitionerClass(CourseAndStudentNaturalKeyPartitioner.class);
    job.setSortComparatorClass(CourseAndStudentKeyComparator.class);
    job.setGroupingComparatorClass(CourseAndStudentNaturalKeyGroupingComparator.class);
    job.setReducerClass(AverageGradeReducer.class);

    job.setOutputKeyClass(CourseAndStudentWritable.class);
    job.setOutputValueClass(IntWritable.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new AverateGradeDriver(), args));
  }
}
