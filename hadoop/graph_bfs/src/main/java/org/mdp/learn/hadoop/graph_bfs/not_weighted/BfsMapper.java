package org.mdp.learn.hadoop.graph_bfs.not_weighted;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BfsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  private LongWritable nodeId = new LongWritable();
  private Text         val    = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Node node = Node.parseNode(value.toString());

    nodeId.set(node.getNodeId());
    val.set("VALUE=" + value.toString());
    context.write(nodeId, val);

    for (Long neighborNodeId : node.getNeighbors()) {
      nodeId.set(neighborNodeId);
      if (Integer.MAX_VALUE == node.getDistanceFromSource()) {
        val.set("INF");
      }
      else {
        val.set(node.getDistanceFromSource() + 1 + "");
      }
      context.write(nodeId, val);
    }
  }

}
