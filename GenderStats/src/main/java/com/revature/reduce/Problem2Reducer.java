package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Notes: The reducer below takes the average cumulative % over the years 2000-2016 
 *        for following the following 8 Gender Statistics categories:
 * 
 * 1) At least Bachelor's or equivalent, population 25+, female => SE.TER.CUAT.BA.FE.ZS
 * 2) At least completed lower secondary, population 25+, female => SE.SEC.CUAT.LO.FE.ZS
 * 3) At least completed post-secondary, population 25+, female => SE.SEC.CUAT.PO.FE.ZS
 * 4) At least completed primary, population 25+ years, female => SE.PRM.CUAT.FE.ZS 
 * 5) At least completed short-cycle tertiary, population 25+, female => SE.TER.CUAT.ST.FE.ZS
 * 6) At least completed upper secondary, population 25+, female => SE.SEC.CUAT.UP.FE.ZS
 * 7) At least Master's or equivalent, population 25+, female => SE.TER.CUAT.MS.FE.ZS 
 * 8) Doctoral or equivalent, population 25+, female => SE.TER.CUAT.DO.FE.ZS
 * 
 * Upon input of these averages the reducer's job is to provide a total average
 * over all categories, with the aim of presenting a reasonably robust metric for
 * "average increase in female education in the U.S. from the year 2000" 
 * 
 * RESEARCH: https://nces.ed.gov/fastfacts/display.asp?id=27
 * 
 * As we can see from the above research, "Educational attainment refers to the highest level of 
 * education completed (defined here as a high school diploma or equivalency certificate, 
 * an associate's degree, a bachelor's degree, or a master's or higher degree).
 * 
 * "Between 2000 and 2017, educational attainment rates among 25- to 29-year-olds increased at 
 * each attainment level. During this time, the percentage who had received at least a high school 
 * diploma or its equivalent increased from 88 to 92 percent, the percentage with an associate's
 * or higher degree increased from 38 to 46 percent, the percentage with a bachelor's or higher 
 * degree increased from 29 to 36 percent, and the percentage with a master's or higher degree 
 * increased from 5 to 9 percent."
 * 
 *
 */

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
