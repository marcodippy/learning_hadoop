package org.mdp.learn.hadoop.page_rank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;

public class PrDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {

    String INPUT_FILE = args[0], OUTPUT_FILE = args[2];
    Long NUMBER_OF_NODES = Long.parseLong(args[1]), MAX_ITERATIONS = Long.parseLong(args[3]);

    long changedPageRanks = -1, lostPageRankMass = -1, iterations = 0;

    String tmpInputFile = INPUT_FILE;
    String tmpOutputFile = OUTPUT_FILE + iterations;
    
    do {
      PageRankJob pageRank = new PageRankJob(tmpInputFile, tmpOutputFile, NUMBER_OF_NODES).run();
      pageRank.printStatus();

      changedPageRanks = pageRank.getChangedPageRankNumber();
      lostPageRankMass = pageRank.getLostPageRankMass();

      if (iterations > 0) HdfsUtils.deleteIfExists(getConf(), new Path(tmpInputFile));
      
      if (lostPageRankMass > 0) {
        tmpInputFile = tmpOutputFile;
        tmpOutputFile = tmpOutputFile + "_rdpr";

        RedistributeLostPageRank redistributeLostPageRank = new RedistributeLostPageRank(tmpInputFile, tmpOutputFile, NUMBER_OF_NODES, lostPageRankMass).run();
        changedPageRanks = redistributeLostPageRank.getChangedPageRankNumber(); // ???
        
        HdfsUtils.deleteIfExists(getConf(), new Path(tmpInputFile));
      }

      
      iterations++;
      tmpInputFile = tmpOutputFile;
      tmpOutputFile = OUTPUT_FILE + iterations;
    }
    while (changedPageRanks > 0 && iterations < MAX_ITERATIONS);

    HdfsUtils.rename(getConf(), new Path(tmpInputFile), new Path(OUTPUT_FILE));
    
    System.out.println("Job finished in " + iterations + " iterations");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PrDriver(), args));
  }
}
