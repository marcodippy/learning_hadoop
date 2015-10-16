package org.mdp.learn.hadoop.average_grade;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPair implements WritableComparable<IntPair> {
	private int count;
	private int sum;

	@Override
	public void readFields(DataInput in) throws IOException {
		count = in.readInt() + Integer.MIN_VALUE;
		sum = in.readInt() + Integer.MIN_VALUE;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(count - Integer.MIN_VALUE);
		out.writeInt(sum - Integer.MIN_VALUE);
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof IntPair) {
			IntPair r = (IntPair) right;
			return r.count == count && r.sum == sum;
		} else {
			return false;
		}
	}

	/** A Comparator that compares serialized IntPair. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(IntPair.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(IntPair.class, new Comparator());
	}

	@Override
	public int compareTo(IntPair o) {
		if (count != o.count) {
			return count < o.count ? -1 : 1;
		} else if (sum != o.sum) {
			return sum < o.sum ? -1 : 1;
		} else {
			return 0;
		}
	}

	public void set(int count, int sum) {
		this.sum = sum;
		this.count = count;
	}

	public int getSum() {
		return sum;
	}

	public int getCount() {
		return count;
	}

}