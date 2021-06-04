package com.rowhighest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RowReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

	@Override
	protected void reduce(LongWritable row, Iterable<LongWritable> values,
			Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		long max=-1;
		for(LongWritable num:values) {
			if(num.get()>=max) {
				max=num.get();
			}
		}
		
		context.write(row,new LongWritable(max));
	}
	
}
