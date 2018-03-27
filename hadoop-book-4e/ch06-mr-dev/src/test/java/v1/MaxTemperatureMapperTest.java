package v1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Ignore;
import org.junit.Test;

public class MaxTemperatureMapperTest {
	/**
	 * mapper 测试类
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
		// Year ^^^^
				"99999V0203201N00261220001CN9999999N9-00111+99999999999");
		// Temperature ^^^^^
		new MapDriver<LongWritable, Text, Text, IntWritable>().withMapper(new MaxTemperatureMapper())
				.withInput(new LongWritable(0), value)
				.withOutput(new Text("1950"), new IntWritable(-11)) // 期望得到的结果
				.runTest();
	}

	// ^^ MaxTemperatureMapperTestV1
	@Ignore // since we are showing a failing test in the book
	// vv MaxTemperatureMapperTestV1Missing
	@Test
	public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
		// Year ^^^^
				"99999V0203201N00261220001CN9999999N9+99991+99999999999");
		// Temperature ^^^^^
		new MapDriver<LongWritable, Text, Text, IntWritable>().withMapper(new MaxTemperatureMapper())
				.withInput(new LongWritable(0), value).runTest();
	}

}
