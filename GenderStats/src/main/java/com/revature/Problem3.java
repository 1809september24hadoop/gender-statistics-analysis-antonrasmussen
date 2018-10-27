package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.revature.map.Problem3Mapper;
import com.revature.reduce.Problem3Reducer;


public class Problem3 extends Configured implements Tool{
	
	public static final String CLASS_NAME = "Problem 3";
	public static final String USAGE_MESSAGE = "Usage: " 
								+ CLASS_NAME 
								+ " <input dir> <output dir>\n";

	public static void main(String[] args) throws Exception {
		int exitFlag = ToolRunner.run(new Problem3(), args);

		if (args.length != 2) {
			System.out.printf(USAGE_MESSAGE);
			System.exit(-1);
		}

		System.exit(exitFlag);
	}

	@Override
	public int run(String[] args) throws Exception {

		final String INPUT_FILE = args[0];
		final String OUTPUT_FILE = args[1];

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, CLASS_NAME);
		job.setJarByClass(getClass());
		job.setMapperClass(Problem3Mapper.class);


		// Specifying combiner class
		job.setCombinerClass(Problem3Reducer.class);
		job.setReducerClass(Problem3Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(job, new Path(INPUT_FILE));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FILE));
		return job.waitForCompletion(true) ? 0 : 1;
	}



}