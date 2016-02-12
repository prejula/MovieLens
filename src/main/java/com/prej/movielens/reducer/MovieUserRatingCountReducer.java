package com.prej.movielens.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieUserRatingCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int count = 0;

		for (Iterator<IntWritable> iterator = values.iterator(); iterator.hasNext();) {

			count += 1;
		}

		context.write(key, new IntWritable(count));
	}
}