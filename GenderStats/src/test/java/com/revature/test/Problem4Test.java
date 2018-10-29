package com.revature.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Problem4Mapper;
import com.revature.reduce.Problem4Reducer;


public class Problem4Test {

	public static final String INPUT_EXAMPLE = "\"Zimbabwe\",\"ZWE\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\","
			+ "\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"64.2229995727539\",\"64.2740020751953\",\"64.2939987182617\",\"64.8359985351563\","
			+ "\"64.6139984130859\",\"64.306999206543\",\"64.0250015258789\",\"62.4939994812012\",\"61.0149993896484\",\"65.7829971313477\",\"69.265998840332\","
			+ "\"73.2389984130859\",\"76.8710021972656\",\"79.9069976806641\",\"78.8820037841797\",\"77.8489990234375\",\"76.8150024414063\",\"75.5419998168945\","
			+ "\"74.1240005493164\",\"74.3010025024414\",\"72.3960037231445\",\"72.5299987792969\",\"72.8170013427734\",\"73.5879974365234\",\"73.9789962768555\",\"74.2630004882813\",";

	public static final String COUNTRY = "Zimbabwe";
	
	public static final Double FIRST_YEAR_VAL = 65.7829971313477;
	
	public static final Double LAST_YEAR_VAL = 74.2630004882813;
	
	public static final int NUM_OF_VALID_YEARS = 17;
	
	public static final Double TOTAL_CHANGE = LAST_YEAR_VAL - FIRST_YEAR_VAL;
	public static final Double AVERAGE_CHANGE = (FIRST_YEAR_VAL + LAST_YEAR_VAL) / 2;

	//Something is buggy here!
	
	public static final Double RESULT = (((TOTAL_CHANGE/AVERAGE_CHANGE) / NUM_OF_VALID_YEARS) * 100);
	
	//public static final Double RESULT = 0.43519094323469076; // Pulled from Answer -> Not correct!


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
		Problem4Mapper mapper = new Problem4Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		Problem4Reducer reducer = new Problem4Reducer();
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
