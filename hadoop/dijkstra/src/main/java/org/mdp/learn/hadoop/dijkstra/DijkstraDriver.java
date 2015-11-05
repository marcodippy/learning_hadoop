package org.mdp.learn.hadoop.dijkstra;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;

public class DijkstraDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    String inputFile = args[0], outputFile = args[1];
    
    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));
    Long changedNodes = -1l, iterations = 0l;
    String tmpInputFile = inputFile, tmpOutputFile = outputFile+iterations;
    
    do {
      Job job = getDijkstraJob(tmpInputFile, tmpOutputFile);
      
      if (!job.waitForCompletion(true)) {
        return 1;
      }
      
      if (iterations > 0) HdfsUtils.deleteIfExists(getConf(), new Path(tmpInputFile));
      
      changedNodes = job.getCounters().findCounter(DijkstraCounters.CHANGED_NODES).getValue();
      
      iterations++;
      tmpInputFile = tmpOutputFile;
      tmpOutputFile = outputFile+iterations;
    } while (changedNodes != 0);

    HdfsUtils.rename(getConf(), new Path(tmpInputFile), new Path(outputFile));
    
    return 0;
  }
  
  private Job getDijkstraJob (String inputFile, String outputFile) throws IllegalArgumentException, IOException {
    Job job = Job.getInstance(getConf(), "Dijkstra");
    job.setJarByClass(DijkstraDriver.class);
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    
    job.setMapperClass(DijkstraMapper.class);
    job.setReducerClass(DijkstraReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));
    
    return job;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DijkstraDriver(), args));
  }
}
