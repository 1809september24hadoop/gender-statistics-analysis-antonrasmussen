package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class Problem2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public static final double NUM_OF_CATEGORIES = 8; // See notes above
	public NumberFormat formatter = new DecimalFormat("#0.0000");
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> annualPercentIncrease, Context context)
			throws IOException, InterruptedException {

		double sumOfIncreaseOverEachCategory = 0;
		double totalAverage = 0;
		
		
		for(DoubleWritable value : annualPercentIncrease) {
			// Get the cumulative over every category
			sumOfIncreaseOverEachCategory += value.get();
		}
		
		// Get the average of averages? NO! Simpson's paradox!!
		// We get the average of the annual percent increases, 
		// which is an average of a `weighted` average		
		totalAverage = ( (sumOfIncreaseOverEachCategory) / NUM_OF_CATEGORIES );
		totalAverage = Double.parseDouble(formatter.format(totalAverage));

		context.write(key, new DoubleWritable(totalAverage));
	}
}
