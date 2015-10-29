package org.mdp.learn.hadoop.relational_joins.map_side;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class MsMapper extends Mapper<Text, TupleWritable, NullWritable, Text> {
  private String SEPARATOR;
  private Text   joinedRow = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    SEPARATOR = context.getConfiguration().get("separator");
  }

  @Override
  protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
    String text = key.toString() + SEPARATOR;

    Iterator<Writable> iterator = value.iterator();
    while (iterator.hasNext()) {
      text += iterator.next().toString();
      if (iterator.hasNext()) text += SEPARATOR;
    }

    joinedRow.set(text);

    context.write(NullWritable.get(), joinedRow);
  }

}
