package com.learn;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HfReducer extends Reducer<IntWritable,Text,Text,IntWritable>{

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		HashMap<String, Integer> hashMap=new HashMap<String, Integer>();
		for(Text word:values) {
			if(hashMap.containsKey(word.toString())) {
				Integer prev=hashMap.get(word.toString());
				hashMap.put(word.toString(),prev+1);
			}else {
				hashMap.put(word.toString(),new Integer(1));
			}
		}
		
		Integer maxu=-1;
		Collection<Integer> allValues=hashMap.values();
		for(Integer val:allValues) {
			if(val>=maxu) {
				maxu=val;
			}
		}
		
		for(Entry<String, Integer> x:hashMap.entrySet()) {
			if(x.getValue()==maxu) {
				context.write(new Text(x.getKey()), new IntWritable(x.getValue()));
			}
		}
		
		// context.write(new Text("Hello world"), new IntWritable(100));
		
		
	}
	
}
