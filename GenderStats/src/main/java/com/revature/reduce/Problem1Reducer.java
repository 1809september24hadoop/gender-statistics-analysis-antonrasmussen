package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class Problem1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	NumberFormat formatter = new DecimalFormat("#0.00");
	static int numOfYears = 0;

	/*
	 * The reduce method runs once for each key received from
	 * the shuffle and sort phase of the MapReduce framework.
	 * The method receives a key of type Text, a set of values of type
	 * IntWritable, and a Context object.
	 */
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> years, Context context)
			throws IOException, InterruptedException {
		
		double yearTotal = 0;
	    

		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (DoubleWritable year : years) {
			
			// Fix this
			if(Double.parseDouble(year.toString()) > 0){
				numOfYears += 1;
			}
			/*
			 * Get the sum of each year.
			 */
			yearTotal += year.get();
		}
		
		Double average = yearTotal/numOfYears;
		
		average = Double.parseDouble(formatter.format(average));
		
		if(average < 30 && average != 0){
			context.write(key, new DoubleWritable(average));
		}

	}
}