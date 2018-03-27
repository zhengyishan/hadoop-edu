package com.hadoopbook.ch05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class MapWritableTest extends WritableTestBase {

	@Test
	public void mapWritable() throws IOException {
       MapWritable src = new MapWritable();
       src.put(new IntWritable(1), new Text("cat"));
       src.put(new VIntWritable(2), new LongWritable(163));
       
       MapWritable dest = new MapWritable();
       WritableUtils.cloneInto(dest, src);
       Assert.assertThat((Text)dest.get(new IntWritable(1)),CoreMatchers.is(new Text("cat")));
       Assert.assertThat((LongWritable) dest.get(new VIntWritable(2)),
    		   CoreMatchers.is(new LongWritable(163)));
	}
	

	  @Test
	  public void setWritableEmulation() throws IOException {
	    MapWritable src = new MapWritable();
	    src.put(new IntWritable(1), NullWritable.get());
	    src.put(new IntWritable(2), NullWritable.get());
	    
	    MapWritable dest = new MapWritable();
	    WritableUtils.cloneInto(dest, src);
	    Assert.assertThat(dest.containsKey(new IntWritable(1)), CoreMatchers.is(true));
	  }

}
