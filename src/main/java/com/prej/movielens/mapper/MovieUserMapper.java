package com.prej.movielens.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieUserMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
		Integer mapKey = stringTokenizer.hasMoreTokens() ? Integer.valueOf(stringTokenizer.nextToken()) : null;
		
		stringTokenizer.nextToken();
		
		Integer mapValue = stringTokenizer.hasMoreTokens() ? Integer.valueOf(stringTokenizer.nextToken()) : null;
		
		context.write(new IntWritable(mapKey), new IntWritable(mapValue));
	}
}