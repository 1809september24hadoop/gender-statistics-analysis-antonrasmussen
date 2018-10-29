package com.revature.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Problem2Mapper;
import com.revature.reduce.Problem2Reducer;


public class Problem2Test {

	public static final String INPUT_EXAMPLE = "\"United States\",\"USA\",\"Educational attainment, "
			+ "at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"31.39076\",\"32.00147\",\"32.67396\",\"\",\n";

	public static final String COUNTRY = "United States";
	
	public static final Double LAST_YEAR_VAL = 32.67396;
	public static final Double FIRST_YEAR_VAL = 31.39076;
	public static final int NUM_OF_VALID_YEARS = 3;
	
	public static final Double TOTAL_CHANGE = LAST_YEAR_VAL - FIRST_YEAR_VAL;
	public static final Double AVERAGE_CHANGE = (FIRST_YEAR_VAL + LAST_YEAR_VAL) / 2;


	public static final Double RESULT = (((TOTAL_CHANGE/AVERAGE_CHANGE) / NUM_OF_VALID_YEARS) * 100);


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
		Problem2Mapper mapper = new Problem2Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		Problem2Reducer reducer = new Problem2Reducer();
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
