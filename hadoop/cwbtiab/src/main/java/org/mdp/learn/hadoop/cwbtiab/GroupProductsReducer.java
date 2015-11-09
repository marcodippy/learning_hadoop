package org.mdp.learn.hadoop.cwbtiab;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupProductsReducer extends Reducer<Text, Text, NullWritable, Text> {
  private Text val = new Text();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    String prds = "";
    Iterator<Text> it = values.iterator();

    while (it.hasNext()) {
      prds += it.next().toString();
      if (it.hasNext()) prds += ",";
    }

    val.set(key.toString()+"-"+prds);
    context.write(NullWritable.get(), val);
  }

}
