package com.prej.movielens.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String input = value.toString();
		StringTokenizer stringTokenizer = new StringTokenizer(input);
		stringTokenizer.nextToken();
		
		int mapperKey = Integer.valueOf(StringUtils.trim(stringTokenizer.nextToken()));
		String mapperValue = StringUtils.trim(stringTokenizer.nextToken());
		
		context.write(new IntWritable(mapperKey), new Text(mapperValue));
	}
}