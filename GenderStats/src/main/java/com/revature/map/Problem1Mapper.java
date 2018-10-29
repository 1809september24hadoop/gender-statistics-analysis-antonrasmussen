package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * PROBLEM #1: Identify the countries where % of female graduates is less than 30%. 
 *
 * 
 * @author Anton Rasmussen
 *
 */

public class Problem1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	public static final String HEADER_ROW_TOKEN = "Country Name";
	public static final String CSV_SPLITTER = "\",\"";

	public static final String CSV_LAST_COL = "\",";
	public static final String REPLACE_WITH = "0";
	
	//Educational attainment, at least completed upper secondary, 
	//population 25+, female (%) (cumulative)
	public static final String INDICATOR_CODE = "SE.SEC.CUAT.UP.FE.ZS";
	
	public static final int FIRST_YEAR = 4; // Col = 1960	
	public static final int LAST_YEAR = 60; // Col = 2016


	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//Filter out Header Row
		if(!value.toString().contains(HEADER_ROW_TOKEN)) {

			String[] row = value.toString().trim().split(CSV_SPLITTER);
			String outputKey = row[0]; //Country Name
			String indicatorCode = row[3]; //For filtering
			String lastColumn = row[LAST_YEAR]; //Last Year Col = 2016
			
			Double lastColDub = Double.parseDouble(
					lastColumn.replace(CSV_LAST_COL, REPLACE_WITH));
			
			lastColumn = lastColDub.toString();
		
			double yearPercent = new Double(0);
			
			//Considering countries where the percentage of female graduates 
			//constitutes at least completing upper secondary
			if(indicatorCode.equals(INDICATOR_CODE))
			{
				for (int i = FIRST_YEAR; i < LAST_YEAR; i++){
					if(!row[i].isEmpty())
					{
						//Get most recent year's cumulative %
						yearPercent = new Double(row[i]);

					}
					//Check for values >= 2 digits and remove the trailing ",
					if(lastColDub > 0 && lastColumn.length() >= 2){
						yearPercent = new Double(
								lastColumn.substring(0, lastColumn.length() - 2));
					}
					//Check for values = 1 digit and remove the trailing ",
					else if(lastColDub > 0) {
						yearPercent = new Double(lastColumn.substring(0, 2));
					}
				}

				context.write(new Text(outputKey.substring(1)), 
						      new DoubleWritable(yearPercent));
			}
		}
	}
}