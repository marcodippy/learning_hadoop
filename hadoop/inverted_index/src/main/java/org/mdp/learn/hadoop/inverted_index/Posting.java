package org.mdp.learn.hadoop.inverted_index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Posting implements WritableComparable<Posting> {
  private Text documentId;
  private Text payload;

  public Posting() {
    documentId = new Text();
    payload = new Text();
  }

  public Posting(Posting p) {
    set(p);
  }

  public Posting(String documentId, String payload) {
    set(documentId, payload);
  }

  public void set(String documentId, String payload) {
    this.documentId = new Text(documentId);
    this.payload = new Text(payload);
  }

  public void set(Posting posting) {
    set(posting.documentId.toString(), posting.payload.toString());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    documentId.write(out);
    payload.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    documentId.readFields(in);
    payload.readFields(in);
  }

  @Override
  public int compareTo(Posting o) {
    int cmp = documentId.compareTo(o.documentId);
    return cmp != 0 ? cmp : payload.compareTo(payload);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((documentId == null) ? 0 : documentId.hashCode());
    result = prime * result + ((payload == null) ? 0 : payload.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Posting) {
      Posting o = (Posting) obj;
      return this.documentId.equals(o.documentId) && this.payload.equals(o.payload);
    }
    return false;
  }

  @Override
  public String toString() {
    return "(" + documentId + ", " + payload + ")";
  }

}
