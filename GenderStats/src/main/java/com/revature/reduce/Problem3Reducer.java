package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * 
 *
 */

public class Problem3Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public NumberFormat formatter = new DecimalFormat("#0.0000");
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> annualPercentChange, Context context)
			throws IOException, InterruptedException {

		double percentChange = 0;
		
		for(DoubleWritable value : annualPercentChange) {
			// Get the percentChange for each country
			percentChange = value.get();
		}
		
		percentChange = Double.parseDouble(formatter.format(percentChange));

		context.write(key, new DoubleWritable(percentChange));
	}
}
