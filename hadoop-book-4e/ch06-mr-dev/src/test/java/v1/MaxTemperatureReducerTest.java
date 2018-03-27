package v1;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class MaxTemperatureReducerTest {
    /**
     * v1 
     * @throws IOException
     * @throws InterruptedException
     */
	@Test
	public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
          new ReduceDriver<Text,IntWritable,Text,IntWritable>()
          .withReducer(new MaxTemperatureReducer())
          .withInput(new Text("1950"),Arrays.asList(new IntWritable(10),new IntWritable(5)))
          .withOutput(new Text("1950"),new  IntWritable(10))
          .runTest();
	}

}
