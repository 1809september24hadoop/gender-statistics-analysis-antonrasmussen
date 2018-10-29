package com.revature.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Problem1Mapper;
import com.revature.reduce.Problem1Reducer;


public class Problem1Test {
	
	public static final String ROW_INPUT_EXAMPLE = "\"Indonesia\",\"IDN\",\"Educational attainment, "
			+ "at least completed upper secondary, population 25+, female (%) (cumulative)\","
			+ "\"SE.SEC.CUAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"3.3549\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"22.75254\",\"21.68805\",\"22.38948\",\"23.837\","
			+ "\"\",\"25.16081\",\"\",\"\",\"27.37407\",\"28.52519\",\"\",\n";
	
	public static final String COUNTRY = "Indonesia";
	public static final Double NON_FORMATTED_DOUBLE = 28.52519;
	

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
		Problem1Mapper mapper = new Problem1Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		Problem1Reducer reducer = new Problem1Reducer();
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
		
		Text sampleRowInput = new Text(ROW_INPUT_EXAMPLE);

		mapDriver.withInput(new LongWritable(1L), sampleRowInput);

		mapDriver.withOutput(new Text(COUNTRY), new DoubleWritable(NON_FORMATTED_DOUBLE));

		mapDriver.runTest();
	}

	/*
	 * Test the reducer.
	 */
	@Test
	public void testReducer() {
		
		Text sampleRowInput = new Text(ROW_INPUT_EXAMPLE);

		mapDriver.withInput(new LongWritable(1L), sampleRowInput);
		
		mapDriver.withOutput(new Text(COUNTRY), new DoubleWritable(NON_FORMATTED_DOUBLE));

		mapDriver.runTest();
	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() {

		Text sampleRowInput = new Text(ROW_INPUT_EXAMPLE);

		mapDriver.withInput(new LongWritable(1L), sampleRowInput);

		mapDriver.withOutput(new Text(COUNTRY), new DoubleWritable(NON_FORMATTED_DOUBLE));

		mapDriver.runTest();
	}
}
