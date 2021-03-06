package com.rowhighest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RowMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>{

	
	private int count=1;
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("Key ="+key.get());
		System.out.println("value ="+value.toString());
		String[] nums=value.toString().split(" ");
		for(String num:nums) {
			context.write(new LongWritable(count), new LongWritable(Integer.valueOf(num)));
		}
		
		count++;
		
	}
	
}
