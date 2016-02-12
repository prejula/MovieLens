package com.prej.movielens.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingCountReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

	@Override
	 public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		int count = 0;
		System.out.println(key);
		Text movieName = null;
		while(it.hasNext())
		{
			String mapValue = it.next().toString();
			
			if (StringUtils.isNumeric(mapValue))
			{
				count = count + 1;
			}
			else
			{
				System.out.println("get movie name : " + mapValue);
				movieName = new Text(mapValue);
			}
		}
		
		context.write(new Text(movieName), new IntWritable(count));
	 }
}
