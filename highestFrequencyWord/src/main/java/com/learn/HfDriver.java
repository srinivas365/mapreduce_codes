package com.learn;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class HfDriver {
	public static void main(String[] args) throws Exception {
		String ipath=args[0];
		String opath=args[1];
		
		Path inputPath=new Path(ipath);
		Path outputPath=new Path(opath);
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setMapperClass(HfMapper.class);
		job.setReducerClass(HfReducer.class);
		job.setJarByClass(HfDriver.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath,true);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
