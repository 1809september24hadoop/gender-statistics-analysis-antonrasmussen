package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Problem5Mapper;
import com.revature.reduce.Problem5Reducer;


public class Problem5 {


	public static final String CLASS_NAME = "Problem 5";
	public static final String USAGE_MESSAGE = "Usage: " 
			+ CLASS_NAME 
			+ " <input dir> <output dir>\n";

	public static void main(String[] args) throws Exception {

		final String INPUT_FILE = args[0];
		final String OUTPUT_FILE = args[1];

		/*
		 * The expected command-line arguments are the paths containing
		 * input and output data. Terminate the job if the number of
		 * command-line arguments is not exactly 2.
		 */
		if (args.length != 2) {
			System.out.printf(USAGE_MESSAGE);
			System.exit(-1);
		}

		/*
		 * Instantiate a Job object the job's configuration.  
		 */
		Job job = new Job();
		job.setJobName(CLASS_NAME);

		/*
		 * Specifies the jar file that contains the driver, mapper, and reducer.
		 * 
		 * Hadoop will transfer this jar file to nodes in the cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(Problem5.class);


		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job, new Path(INPUT_FILE));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FILE));

		job.setMapperClass(Problem5Mapper.class);
		job.setReducerClass(Problem5Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);


		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}