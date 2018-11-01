package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * PROBLEM #3: List the % of change in male employment from the year 2000. 
 * @author Anton Rasmussen
 *
 */

public class Problem3Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	public static final String HEADER_ROW_TOKEN = "Country Name";
	public static final String CSV_SPLITTER = "\",\"";

	public static final String CSV_LAST_COL = "\",";
	public static final String REPLACE_WITH = "0";

	//Employment to population ratio, 15+, male (%) (modeled ILO estimate)"
	public static final String INDICATOR = "SL.EMP.TOTL.SP.MA.ZS"; 

	public static final int FIRST_YEAR = 44; // Col = 2000
	public static final int LAST_YEAR = 60; // Col = 2016


	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		//Filter out Header Row
		if(!value.toString().contains(HEADER_ROW_TOKEN)) {

			// Splitting the line on double quote and comma
			String[] row = value.toString().trim().split(CSV_SPLITTER);


			String outputKey = row[0]; //Country Name
			String indicatorCode = row[3]; //For filtering			
			String lastColumn = row[LAST_YEAR]; //Last Year Col = 2016

			//Clean up the last column of a CSV file, replace empty strings with 0
			Double lastColDub = Double.parseDouble(lastColumn.replace(CSV_LAST_COL, REPLACE_WITH));
			lastColumn = lastColDub.toString(); //Grabs cleaned value

			//Store last and first column values for calculating value change over time
			Double lastValue = new Double(0);
			Double firstValue = new Double(0);

			//Get the lastValue
			if(indicatorCode.contains(INDICATOR)) {
				for (int i = FIRST_YEAR; i <= LAST_YEAR; i++){
					if(!row[i].isEmpty()) {
						if(i == LAST_YEAR) {
							//Check for values >= 2 digits and remove the trailing ",
							if(lastColDub > 0 && lastColumn.length() >= 2) {
								lastValue = new Double(lastColumn.substring(0, lastColumn.length() - 2));
							}
							//Check for values = 1 digit and remove the trailing ",
							else if(lastColDub > 0) {
								lastValue = new Double(lastColumn.substring(0, 2));
							}

						}
						else {
							lastValue = new Double(row[i]);

						}
						numOfValidYears += 1;
					}
				}
			}



			//Get the firstValue
			if(indicatorCode.contains(INDICATOR)) {
				for (int i = LAST_YEAR; i >= FIRST_YEAR; --i) {
					if(!row[i].isEmpty()) {
						if(i == LAST_YEAR) {
							//Check for values >= 2 digits and remove the trailing ",
							if(lastColDub > 0 && lastColumn.length() >= 2) {
								firstValue = new Double(lastColumn.substring(0, lastColumn.length() - 2));
							}
							//Check for values = 1 digit and remove the trailing ",
							else if(lastColDub > 0) {
								firstValue = new Double(lastColumn.substring(0, 2));
							}
						}
						else {
							firstValue = new Double(row[i]);
						}
					}
				}
			}

			//Get the average increase for each category

			Double totalChange = lastValue - firstValue;
			Double averageChange = ((lastValue + firstValue) / 2);

			Double annualPercentChange = (totalChange/averageChange);


			//Filter by INDICATOR and filter OUT countries with NO Data
			if(indicatorCode.contains(INDICATOR)) {
				context.write(new Text(outputKey.substring(1)), new DoubleWritable(annualPercentChange));
			}


		}
	}
}


