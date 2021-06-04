package com.learn;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HfMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String words[]=value.toString().split(" ");
		for(String word:words) {
			context.write(new IntWritable(1),new Text(word));
		}
		// context.write(new IntWritable(1234),new Text("Hello world"));
	}

	
	
}
