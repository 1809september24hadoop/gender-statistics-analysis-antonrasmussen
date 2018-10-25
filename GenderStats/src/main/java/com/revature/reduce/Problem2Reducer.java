package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Problem2Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			
			int maxSalesValue = Integer.MIN_VALUE;
			for(IntWritable val : values) {
				maxSalesValue = Math.max(maxSalesValue, val.get());
			}  
			result.set(maxSalesValue);
			context.write(key, result);
		}
	}
