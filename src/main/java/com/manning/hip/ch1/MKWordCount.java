package com.manning.hip.ch1;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public final class MKWordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}
	
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			context.write(key, new IntWritable(sum));
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		
        String[] input = Arrays.copyOfRange(args, 0, args.length - 1);
        String output = args[args.length - 1];
		
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "wordcount");
		
	    job.setJarByClass(MKWordCount.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
	    Path outputPath = new Path(output);

	    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
	    FileOutputFormat.setOutputPath(job, outputPath);
	   
		
		job.waitForCompletion(true);
		
	}
	
}
