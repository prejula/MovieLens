package com.prej.movielens;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.prej.movielens.mapper.MovieNameMapper;
import com.prej.movielens.mapper.RatingMapper;
import com.prej.movielens.reducer.MovieRatingCountReducer;


/**
 * Hello world!
 *
 */
public class ListRatingCountDriver extends Configured implements Tool {
  
	public static void main( String[] args ) throws Exception
    {
		int exitCode = ToolRunner.run(new ListRatingCountDriver(), args);
	    System.exit(exitCode);
    }
	public int run(String[] args) throws Exception {
	
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());
	            
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieNameMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    job.setReducerClass(MovieRatingCountReducer.class);
	   
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	  
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}
