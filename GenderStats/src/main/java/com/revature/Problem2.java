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

import com.revature.map.Problem2Mapper;
import com.revature.reduce.Problem2Reducer;


public class Problem2 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitFlag = ToolRunner.run(new Problem2(), args);

		if (args.length != 2) {
			System.out.printf(
					"Usage: Problem2 <input dir> <output dir>\n");
			System.exit(-1);
		}

		System.exit(exitFlag);
	}

	@Override
	public int run(String[] args) throws Exception {

		final String INPUT_FILE = args[0];
		final String OUTPUT_FILE = args[1];

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Problem2");
		job.setJarByClass(getClass());
		job.setMapperClass(Problem2Mapper.class);


		// Specifying combiner class
		job.setCombinerClass(Problem2Reducer.class);
		job.setReducerClass(Problem2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(job, new Path(INPUT_FILE));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FILE));
		return job.waitForCompletion(true) ? 0 : 1;
	}



}