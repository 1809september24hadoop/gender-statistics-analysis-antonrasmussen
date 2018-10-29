package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 

public class Problem5Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> mostCurrentYearDifference, Context context)
			throws IOException, InterruptedException {
		 
		double currYearDifference = 0;	    
		
		for(DoubleWritable value : mostCurrentYearDifference) {
			currYearDifference = value.get();
		}
		
		context.write(key, new DoubleWritable(currYearDifference));
						
		//TODO: Filter out countries with < 1 -> Indicates it takes at least one day less time for Males than Females
		//if(currYearDifference >= 1){
		//	context.write(key, new DoubleWritable(currYearDifference));
		//}
		
		

	}
}