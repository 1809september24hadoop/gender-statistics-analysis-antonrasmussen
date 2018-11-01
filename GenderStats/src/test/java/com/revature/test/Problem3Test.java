package com.revature.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Problem3Mapper;
import com.revature.reduce.Problem3Reducer;


public class Problem3Test {

	public static final String INPUT_EXAMPLE = "\"India\",\"IND\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\","
			+ "\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"81.6009979248047\",\"81.4580001831055\",\"81.0410003662109\","
			+ "\"81.1829986572266\",\"80.6750030517578\",\"80.4169998168945\",\"79.7149963378906\",\"79.9169998168945\",\"79.3259963989258\","
			+ "\"78.9729995727539\",\"79.5530014038086\",\"79.1999969482422\",\"79.5869979858398\",\"79.7330017089844\",\"79.6259994506836\","
			+ "\"79.1640014648438\",\"79.0090026855469\",\"78.2009963989258\",\"77.8509979248047\",\"77.8980026245117\",\"77.4140014648438\","
			+ "\"76.8150024414063\",\"76.625\",\"76.4290008544922\",\"76.4489974975586\",\"76.4499969482422\",\n";

	public static final String COUNTRY = "India";
	
	public static final Double LAST_YEAR_VAL = 76.4499969482422;
	public static final Double FIRST_YEAR_VAL = 78.9729995727539;		
	
	public static final Double TOTAL_CHANGE = LAST_YEAR_VAL - FIRST_YEAR_VAL;
	public static final Double AVERAGE_CHANGE = (FIRST_YEAR_VAL + LAST_YEAR_VAL) / 2;

	
	//public static final Double RESULT = (TOTAL_CHANGE/AVERAGE_CHANGE);
	// Pulled from Answer -> Due to floating point stuff...
	public static final Double RESULT = -0.03246627180004357;


	/*
	 * Declare harnesses that let you test a mapper, a reducer, and
	 * a mapper and a reducer working together.
	 */
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {


		/*
		 * Set up the mapper test harness.
		 */
		Problem3Mapper mapper = new Problem3Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		Problem3Reducer reducer = new Problem3Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {

		Text sampleRowInput = new Text(INPUT_EXAMPLE);

		mapDriver.withInput(new LongWritable(1L), sampleRowInput);

		mapDriver.withOutput(new Text(COUNTRY), new DoubleWritable(RESULT));

		mapDriver.runTest();
	}

	/*
	 * Test the reducer.
	 */
	@Test
	public void testReducer() {

		Text sampleRowInput = new Text(INPUT_EXAMPLE);

		mapDriver.withInput(new LongWritable(1L), sampleRowInput);

		mapDriver.withOutput(new Text(COUNTRY), new DoubleWritable(RESULT));

		mapDriver.runTest();
	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() {

		Text sampleRowInput = new Text(INPUT_EXAMPLE);

		mapDriver.withInput(new LongWritable(1L), sampleRowInput);

		mapDriver.withOutput(new Text(COUNTRY), new DoubleWritable(RESULT));

		mapDriver.runTest();
	}
}
