package com.freitas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Decompressor extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		conf.set("mapreduce.map.memory.mb", "7373");
		conf.set("mapreduce.map.java.opts", "-Xmx6144m");
		conf.set(CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY, 
				"org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapreduce.compress.map.output", "false");
		Job job = Job.getInstance(conf);
		job.setJobName("Decompress");
		job.setJarByClass(Decompressor.class);
		job.setMapperClass(DecompressorMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static Job createWithConf(Configuration conf, String service, String input, String output) throws Exception {
		conf.set("mapreduce.map.memory.mb", "7373");
		conf.set("mapreduce.map.java.opts", "-Xmx6144m");
		Job job = Job.getInstance(conf, "Decompress " + service);
		job.setJarByClass(Decompressor.class);
		job.setMapperClass(DecompressorMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Decompressor(), args);
		System.exit(res);
	}
	
	
	public static class DecompressorMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
			context.write(value, NullWritable.get());
		}
	}
	
}
