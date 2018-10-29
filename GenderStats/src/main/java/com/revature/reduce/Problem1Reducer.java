package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 

public class Problem1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	NumberFormat formatter = new DecimalFormat("#0.00");
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> mostCurrentYear, Context context)
			throws IOException, InterruptedException {
		 
		double currYearPercent = 0;	    
		
		for(DoubleWritable value : mostCurrentYear) {
			currYearPercent = value.get();
		}
				
		currYearPercent = Double.parseDouble(formatter.format(currYearPercent));
		
		//Filter out countries with >= 30% (Cumulative)
		if(currYearPercent < 30 && currYearPercent != 0){
			context.write(key, new DoubleWritable(currYearPercent));
		}
		
		

	}
}