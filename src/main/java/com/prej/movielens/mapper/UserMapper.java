package com.prej.movielens.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.prej.movielens.TextPair;

public class UserMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 
		 StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
		 String userId = stringTokenizer.hasMoreTokens() ? stringTokenizer.nextToken() : null;
		 stringTokenizer.nextToken();
		 String rating = stringTokenizer.hasMoreTokens() ? stringTokenizer.nextToken() : null;

		 context.write(new TextPair(userId, rating), new Text(value));
	 }
}