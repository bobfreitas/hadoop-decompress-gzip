package com.freitas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * This class is used to compress data using the default codec to generate
 * .defalte file.  Its primary use is to generate compressed files that 
 * can be used in the testing, but made it part of the main code, since it
 * might be useful for something sometime.  
 */
public class Compressor extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("Compressor");
		job.setJarByClass(Compressor.class);
		job.setMapperClass(CompressorMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Compressor(), args);
		System.exit(res);
	}
	
	public static class CompressorMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
			context.write(value, NullWritable.get());
		}
	}

}
