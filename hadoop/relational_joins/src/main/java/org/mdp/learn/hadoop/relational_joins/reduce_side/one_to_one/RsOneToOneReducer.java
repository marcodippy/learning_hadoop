package org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RsOneToOneReducer extends Reducer<JoinKey, Text, Text, NullWritable> {
  private NullWritable NULL = NullWritable.get();
  private int          TABLE_NUMBER;
  private Text         text = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    TABLE_NUMBER = context.getConfiguration().getInt("tableNumber", 2);
  }

  @Override
  protected void reduce(JoinKey key, Iterable<Text> rows, Context context) throws IOException, InterruptedException {
    String joinedRow = "";
    int rowCount = 0;

    Iterator<Text> iter = rows.iterator();
    while (iter.hasNext()) {
      rowCount++;
      joinedRow += iter.next().toString();
      if (iter.hasNext()) joinedRow += ";";
    }

    if (rowCount == TABLE_NUMBER) {
      text.set(joinedRow);
      context.write(text, NULL);
    }
  }

}
