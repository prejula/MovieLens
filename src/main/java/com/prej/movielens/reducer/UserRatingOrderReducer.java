package com.prej.movielens.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.prej.movielens.TextPair;

public class UserRatingOrderReducer extends	Reducer<TextPair, Text, NullWritable, Text> {

	public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
			context.write(NullWritable.get(), new Text(key.getFirst() + " " + key.getSecond()));
		}
	}
}
