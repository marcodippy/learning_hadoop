package org.mdp.learn.hadoop.inverted_index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;

public class TermInfo implements WritableComparable<TermInfo> {
  private Text         term;
  private VIntWritable documentId;

  public TermInfo() {
    term = new Text();
    documentId = new VIntWritable();
  }

  public TermInfo(String term, int documentId) {
    set(term, documentId);
  }

  public void set(TermInfo termInfo) {
    set(termInfo.term.toString(), termInfo.documentId.get());
  }

  public void set(String term, int documentId) {
    this.term = new Text(term);
    this.documentId = new VIntWritable(documentId);
  }

  public Text getTerm() {
    return term;
  }

  public VIntWritable getDocumentId() {
    return documentId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    term.write(out);
    documentId.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    term.readFields(in);
    documentId.readFields(in);
  }

  @Override
  public int compareTo(TermInfo o) {
    int cmp = term.compareTo(o.term);
    if (cmp == 0) cmp = documentId.compareTo(o.documentId);
    return cmp;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    result = prime * result + ((documentId == null) ? 0 : documentId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TermInfo) {
      TermInfo o = (TermInfo) obj;
      return term.equals(o.term) && documentId.equals(o.documentId);
    }
    return false;
  }

  @Override
  public String toString() {
    return "(" + term + ", " + documentId + ")";
  }

}
