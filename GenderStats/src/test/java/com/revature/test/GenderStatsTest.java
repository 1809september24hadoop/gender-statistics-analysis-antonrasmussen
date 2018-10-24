package com.revature.test;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mrunit.mapreduce.MapDriver;
//import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
//import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
//import org.junit.Before;
//import org.junit.Test;
//
//import com.revature.map.Problem1Mapper;
//import com.revature.reduce.Problem1Reducer;
//
//
public class GenderStatsTest {
//
//  /*
//   * Declare harnesses that let you test a mapper, a reducer, and
//   * a mapper and a reducer working together.
//   */
//  private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
//  private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
//  private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
//
//  /*
//   * Set up the test. This method will be called before every test.
//   */
//  @Before
//  public void setUp() {
//
//    /*
//     * Set up the mapper test harness.
//     */
//    Problem1Mapper mapper = new Problem1Mapper();
//    mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
//    mapDriver.setMapper(mapper);
//
//    /*
//     * Set up the reducer test harness.
//     */
//    Problem1Reducer reducer = new Problem1Reducer();
//    reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
//    reduceDriver.setReducer(reducer);
//
//    /*
//     * Set up the mapper/reducer test harness.
//     */
//    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
//    mapReduceDriver.setMapper(mapper);
//    mapReduceDriver.setReducer(reducer);
//  }
//
//  /*
//   * Test the mapper.
//   */
//  @Test
//  public void testMapper() {
//
//    /*
//     * For this test, the mapper's input will be "1L CountryName" 
//     */
//    mapDriver.withInput(new LongWritable(1L), new Text("Merica"));
//
//    /*
//     * The expected output is "merica 1", "merica 1", and "canada 1".
//     */
//    mapDriver.withOutput(new Text("merica"), new DoubleWritable(1.0));
//    mapDriver.withOutput(new Text("merica"), new DoubleWritable(1.0));
//    mapDriver.withOutput(new Text("canada"), new DoubleWritable(1.0));
//
//    /*
//     * Run the test.
//     */
//    mapDriver.runTest();
//  }
//
//  /*
//   * Test the reducer.
//   */
//  @Test
//  public void testReducer() {
//
//    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
//    values.add(new DoubleWritable(1.0));
//    values.add(new DoubleWritable(1.0));
//
//    /*
//     * For this test, the reducer's input will be "merica 1 1".
//     */
//    reduceDriver.withInput(new Text("merica"), values);
//
//    /*
//     * The expected output is "merica 2"
//     */
//    reduceDriver.withOutput(new Text("merica"), new DoubleWritable(2.0));
//
//    /*
//     * Run the test.
//     */
//    reduceDriver.runTest();
//  }
//
//  /*
//   * Test the mapper and reducer working together.
//   */
//  @Test
//  public void testMapReduce() {
//
//    /*
//     * For this test, the mapper's input will be "merica merica canada 1" 
//     */
//    mapReduceDriver.withInput(new LongWritable(1), new Text("merica merica canada"));
//
//    /*
//     * The expected output (from the reducer) is "merica 2", "canada 1". 
//     */
//    mapReduceDriver.addOutput(new Text("merica"), new DoubleWritable(2.0));
//    mapReduceDriver.addOutput(new Text("canada"), new DoubleWritable(1.0));
//
//    /*
//     * Run the test.
//     */
//    mapReduceDriver.runTest();
//  }
}
