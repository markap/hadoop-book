package com.manning.hip.ch1;

import java.io.IOException;
import java.util.Arrays;

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


public final class MKBigrammCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String bigramm;
			
			int lineLength = line.length();
			for (int i = 0; i < lineLength; i++) {
			    bigramm = String.valueOf(line.charAt(i));
			    if (i == 0) {
			        String startLetter = " " + bigramm;
			        word.set(startLetter.toString());
			        context.write(word, one);
			    } 
			    
			    if (i + 1 == lineLength) {
			        bigramm += " ";
			    } else {
			        bigramm += String.valueOf(line.charAt(i+1));
			    }
			    
			    word.set(bigramm.toString());
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
		
	    job.setJarByClass(MKBigrammCount.class);
		
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
