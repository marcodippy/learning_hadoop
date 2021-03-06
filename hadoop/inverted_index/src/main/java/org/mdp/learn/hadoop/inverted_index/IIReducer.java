package org.mdp.learn.hadoop.inverted_index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IIReducer extends Reducer<TermInfo, Posting, Text, Postings> {
  private List<Posting> tmpPostings = new ArrayList<>();
  private Postings      postings    = new Postings();
  private TermInfo      prev        = null;
  private int           docId       = 0;

  @Override
  protected void reduce(TermInfo termInfo, Iterable<Posting> values, Context context) throws IOException, InterruptedException {
    for (Posting posting : values) {
      if (prev != null && !termInfo.getTerm().equals(prev.getTerm())) {
        emitPostings(context);
      }

      docId = posting.getDocumentId().get() - docId;

      tmpPostings.add(new Posting(docId, posting.getPayload().toString()));

      if (prev == null) prev = new TermInfo();
      prev.set(termInfo);
    }

    docId = 0;
  }

  private void emitPostings(Context context) throws IOException, InterruptedException {
    postings.set(tmpPostings.toArray(new Posting[tmpPostings.size()]));
    context.write(prev.getTerm(), postings);
    tmpPostings.clear();
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    if (!tmpPostings.isEmpty()) emitPostings(context);
  }

}
