package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * PROBLEM #2: List the average increase in female education in the U.S. from the year 2000
 * @author cloudera
 *
 */

public class Problem2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		//Filter out Header Row
		if(!value.toString().contains("Country Name")) {

			// Splitting the line on tab
			String[] row = value.toString().trim().split("\",\"");
			String outputKey = row[0]; //Country name
			String indicatorCode = row[3]; //For filtering
			String firstColumn = row[44]; //First Year Col = 2000
			String lastColumn = row[60]; //Last Year Col = 2016


			//What does set do???
			//item.set(row[0]);

			Double lastColDub = Double.parseDouble(lastColumn.replace("\",", "0"));
			lastColumn = lastColDub.toString();

			double yearPercent = new Double(0.00);
			//TODO: figure out a way to pass numOfYears to reducer for average 
			//int numOfYears = 0;

			//Considering countries where the percentage of female graduates 
			//constitutes at least completing upper secondary
			if(indicatorCode.contains("SE.SEC.CUAT.UP.FE.ZS"))
			{
				for (int i = 44; i < 60; i++){
					if(!row[i].isEmpty())
					{
						//Get most recent year's cumulative %
						yearPercent = new Double(row[i]);

						//TODO: It might be more interesting to
						//Calculate for average over time
						//yearPercentTotals += new Double(row[i]);
						//numOfYears++;
					}
					//Check for values >= 2 digits and remove the trailing ",
					if(lastColDub > 0 && lastColumn.length() >= 2){
						yearPercent = new Double(lastColumn.substring(0, lastColumn.length() - 2));
					}
					//Check for values = 1 digit and remove the trailing ",
					else if(lastColDub > 0) {
						yearPercent = new Double(lastColumn.substring(0, 2));
					}
				}


				//TODO: Figure out a way to obtain average with MR
				Double education = Double.parseDouble(row[1]);


				context.write(new Text(outputKey.substring(1)), new DoubleWritable(education));
			}
		}
	}

