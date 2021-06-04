package com.learn;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ColumnReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

	@Override
	protected void reduce(LongWritable col, Iterable<LongWritable> values,
			Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		long max=-1;
		for(LongWritable val:values) {
			if(val.get()>max) {
				max=val.get();
			}
		}
		
		context.write(col, new LongWritable(max));
		
	}
	
}
