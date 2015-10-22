package org.mdp.learn.hadoop.moving_average;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovingAverageReducer extends Reducer<MovingAverageKey, Float, Text, Text> {

}
