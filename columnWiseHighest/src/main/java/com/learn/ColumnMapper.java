package com.learn;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ColumnMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String nums[]=value.toString().split(" ");
		long count=0;
		for(String num:nums) {
			context.write(new LongWritable(count), new LongWritable(Long.valueOf(num)));
			count++;
		}
		
	}
	
}
