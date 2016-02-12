package com.prej.movielens;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.prej.movielens.mapper.UserMapper;
import com.prej.movielens.reducer.UserRatingOrderReducer;


/**
 * Hello world!
 *
 */
public class OrderByRatingDriver extends Configured implements Tool {
  
	public static void main( String[] args ) throws Exception
    {
		int exitCode = ToolRunner.run(new OrderByRatingDriver(), args);
	    System.exit(exitCode);
    }
	public int run(String[] args) throws Exception {
	
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());
	            
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(UserMapper.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
	    
	    job.setReducerClass(UserRatingOrderReducer.class);
	   
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	  
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}
