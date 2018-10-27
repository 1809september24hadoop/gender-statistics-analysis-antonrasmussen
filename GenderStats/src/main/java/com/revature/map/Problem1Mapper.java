package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

/**
 * PROBLEM #1: Identify the countries where % of female graduates is less than 30%. 
 * @author cloudera
 *
 */

public class Problem1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	/*
	 * The map method runs once for each line of text in the input file.
	 * The method receives a key of type LongWritable, a value of type
	 * Text, and a Context object.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//Filter out Header Row
		if(!value.toString().contains("Country Name")) {

			String[] row = value.toString().trim().split("\",\"");
			String outputKey = row[0]; //Country Name
			String indicatorCode = row[3]; //For filtering
			String lastColumn = row[60]; //Last Year Col = 2016
			
			Double lastColDub = Double.parseDouble(lastColumn.replace("\",", "0"));
			lastColumn = lastColDub.toString();
		
			double yearPercent = new Double(0.00);
			
			//Considering countries where the percentage of female graduates 
			//constitutes at least completing upper secondary
			if(indicatorCode.contains("SE.SEC.CUAT.UP.FE.ZS"))
			{
				for (int i = 4; i < 60; i++){
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

				context.write(new Text(outputKey.substring(1)), new DoubleWritable(yearPercent));
			}
		}
	}
}