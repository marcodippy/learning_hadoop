package org.mdp.learn.hadoop.graph_bfs.not_weighted;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;

public class BfsDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    String inputFile = args[0], outputFile = args[1];
    
    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));
    
    Long notDiscoveredNodes = -1l, iterations = 0l;
    String tmpInputFile = inputFile, tmpOutputFile = outputFile+iterations;
    
    do {
      Job job = getBfsJob(tmpInputFile, tmpOutputFile);
      
      if (!job.waitForCompletion(true)) {
        return 1;
      }
      
      if (iterations > 0) HdfsUtils.deleteIfExists(getConf(), new Path(tmpInputFile));
      
      notDiscoveredNodes = job.getCounters().findCounter(BfsCounters.NOT_DISCOVERED_NODES).getValue();
      
      iterations++;
      tmpInputFile = tmpOutputFile;
      tmpOutputFile = outputFile+iterations;
    } while (notDiscoveredNodes != 0);
    
    HdfsUtils.rename(getConf(), new Path(tmpInputFile), new Path(outputFile));

    return 0;
  }

  private Job getBfsJob (String inputFile, String outputFile) throws IllegalArgumentException, IOException {
    Job job = Job.getInstance(getConf(), "Parallel Breadth First Search");
    job.setJarByClass(BfsDriver.class);
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    
    job.setMapperClass(BfsMapper.class);
    job.setReducerClass(BfsReducer.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));
    
    return job;
  }
  
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new BfsDriver(), args));
  }
}
