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

public class ColumnMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {


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
			String outputKey = row[0]; // Country Name
			//String category = row[2]; // Category
			String indicatorCode = row[3]; // Indicator Code
			
			double yearTotals = new Double(0.00);
			
			//Considering countries where the percentage of female graduates 
			//constitutes at least completing upper secondary
			if(indicatorCode.contains("SE.SEC.CUAT.UP.FE.ZS"))
			{
				for (int i = 44; i < 60; i++){
					if(!row[i].isEmpty())
					{
						yearTotals += new Double(row[i]);

					}
				}

				context.write(new Text(outputKey.substring(1)), new DoubleWritable(yearTotals));
			}
		}
	}
}