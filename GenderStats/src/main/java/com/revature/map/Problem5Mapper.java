package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * PROBLEM #5: Identify the countries where it takes women longer than men to start a business
 *
 * 
 * @author Anton Rasmussen
 *
 */

public class Problem5Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	public static final String HEADER_ROW_TOKEN = "Country Name";
	public static final String CSV_SPLITTER = "\",\"";

	public static final String CSV_LAST_COL = "\",";
	public static final String REPLACE_WITH = "0";

	//Time required to start up a business, male (days)
	public static final String INDICATOR_CODE_MA = "IC.REG.DURS.MA";

	//Time required to start up a business, female (days)
	public static final String INDICATOR_CODE_FE = "IC.REG.DURS.FE";

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

			Double lastColDouble = Double.parseDouble(
					lastColumn.replace(CSV_LAST_COL, REPLACE_WITH));

			lastColumn = lastColDouble.toString();

			double daysMale = new Double(0);

			double daysFemale = new Double(0);

			if(indicatorCode.equals(INDICATOR_CODE_MA))
			{
				for (int i = FIRST_YEAR; i < LAST_YEAR; i++){
					if(!row[i].isEmpty())
					{
						//Get most recent year days
						daysMale = new Double(row[i]);

					}

					context.write(new Text(outputKey.substring(1) + " " + INDICATOR_CODE_MA), 
							new DoubleWritable(daysMale));

				}
			}
			if(indicatorCode.equals(INDICATOR_CODE_FE))
			{
				for (int i = FIRST_YEAR; i < LAST_YEAR; i++){
					if(!row[i].isEmpty())
					{
						//Get most recent year days
						daysFemale = new Double(row[i]);

					}

					context.write(new Text(outputKey.substring(1) + " " + INDICATOR_CODE_FE), 
							new DoubleWritable(daysFemale));

				}



			}


		}
	}
}