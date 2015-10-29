package org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_many;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one.JoinKey;

public class RsOneToManyReducer extends Reducer<JoinKey, Text, Text, NullWritable> {
  private NullWritable NULL           = NullWritable.get();
  private Text         text           = new Text();
  private JoinKey      currentJoinKey = new JoinKey();
  private List<String> leftRows       = new LinkedList<>();
  private List<String>   rightRows      = new LinkedList<>();

  @Override
  protected void reduce(JoinKey key, Iterable<Text> rows, Context context) throws IOException, InterruptedException {
    if (key.getValue().equals(currentJoinKey.getValue()) && !key.getOriginTable().equals(currentJoinKey.getOriginTable())) {
      rightRows.clear();
      rows.forEach(row -> rightRows.add(row.toString()));

      for (String leftRow : leftRows) {
        for (String rightRow : rightRows) {
          text.set(leftRow + ";" + rightRow);
          context.write(text, NULL);
        }
      }
    }
    else {
      currentJoinKey.set(key);
      leftRows.clear();
      rows.forEach((leftRow) -> leftRows.add(leftRow.toString()));
    }
  }

}
