package com.freitas.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.freitas.avro.AvroDataEncoder;
import com.freitas.commons.HDFSUtilities;
import com.freitas.commons.SerializerConstants;

public class AvroSerializerDriver extends Configured implements Tool {
	
	public enum COUNTERS {
		  PARSE_FAILED,
		  EMPTY_JSON
	}
	
	public static String OOZIE = "oozie";
	public static String OOZIE_ACTION_OUTPUT_PROPERTIES = "oozie.action.output.properties";
	
	private static final Logger LOG = Logger.getLogger(AvroSerializerDriver.class);
	
	String jobName, baseInputPath, baseOutputPath, dataParsingFormat, s3BucketLocation;
	String tableName, outputDirectoryPath, decompressedDirPath;

	public int run(String[] args) throws Exception {
		
		readCommandLineArgs(args);
		
		Configuration conf = this.getConf();
		conf.set(SerializerConstants.S3_BUCKET_LOCATION, s3BucketLocation);
		
		String compressedInputPath = baseInputPath + Path.SEPARATOR + jobName + Path.SEPARATOR;
		String uncompressedOutputPath = decompressedDirPath + Path.SEPARATOR + jobName + Path.SEPARATOR;
		HDFSUtilities.deleteDir(uncompressedOutputPath, conf);
		
		// Do the first job to decompress the incoming data
		Job decompressJob = Decompressor.createWithConf(conf, jobName, compressedInputPath, uncompressedOutputPath);		
		if (!decompressJob.waitForCompletion(true)) {
			throw new RuntimeException("Unable to decompress data going to the serializer");
		}
		
		// Now do the second job to serialize the data
		conf = this.getConf();
		// get the avro schema for the destination Hive table
		String avroSchemaContents = readAvroSchemaFile(tableName, conf); 
		// set some contant config parameters
		conf.set(SerializerConstants.AVRO_SCHEMA, avroSchemaContents);
		conf.set(SerializerConstants.BASE_OUTPUT_PATH, baseOutputPath);
		conf.set(SerializerConstants.S3_BUCKET_LOCATION, s3BucketLocation);
		conf.set(CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY, 
				"org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapreduce.map.memory.mb", "2048");
		conf.set("mapreduce.map.java.opts", "-Xmx1834m");
		conf.setLong(FileInputFormat.SPLIT_MINSIZE, 1073741824L);

		Job serializeJob = Job.getInstance(conf);
		serializeJob.setJobName(jobName + " AvroSerializer");
		serializeJob.setJarByClass(AvroSerializerDriver.class);
		// make sure speculative execution is off because it slows EMR writing to S3
		serializeJob.setSpeculativeExecution(false); 
		serializeJob.setMapperClass(SerializerAvroMapper.class); 
		serializeJob.setNumReduceTasks(0);

		//Input, use the output from the decompressed job
		serializeJob.setInputFormatClass(TextInputFormat.class);
		LOG.info("Setting the Input Path: " + uncompressedOutputPath);
		FileInputFormat.addInputPath(serializeJob, new Path(uncompressedOutputPath));

		//Output
		outputDirectoryPath = getFinalOutputDirectoryPath(baseOutputPath, jobName, tableName);
		FileOutputFormat.setOutputPath(serializeJob, new Path(outputDirectoryPath));
		AvroDataEncoder avroDataEncoder = new AvroDataEncoder();
		Schema avroSchema = avroDataEncoder.parseSchema(avroSchemaContents);
		AvroJob.setOutputKeySchema(serializeJob, avroSchema);
		serializeJob.setOutputFormatClass(AvroKeyOutputFormat.class);

		int status = serializeJob.waitForCompletion(true)? 0 : 1;
		if (status == 0) {
			// job finished successfully
			LOG.info("Job completed successfully");
		}
		//clean up the uncompressed data, no reason to keep it around
		HDFSUtilities.deleteDir(uncompressedOutputPath, conf);
		return status;
	}

	public static String readAvroSchemaFile(String tableName, Configuration conf) throws IOException {
		String s3BucketLocation = conf.get(SerializerConstants.S3_BUCKET_LOCATION);
		String schemaFilePath = s3BucketLocation + Path.SEPARATOR + 
				"avroschemas" + Path.SEPARATOR + tableName + Path.SEPARATOR + 
				tableName + "-latest.avsc";
		LOG.info("The Path of the Avro Schema File is " + schemaFilePath);
		String avroSchemaStr = HDFSUtilities.readFile(schemaFilePath, conf);
		LOG.info("Avro Schema File Read Successfully");
		return avroSchemaStr;
	}

	public String getOutputDirectoryPath() {
		return this.outputDirectoryPath;
	}

	private String getFinalOutputDirectoryPath(String baseOutputPath, String productType, String compressedInputDirName) {
		// Eg:Format = <S#_Bucket>/avroserializedzone/<producttype>/YYYY/MM/DD/HH
		String outputDirName = compressedInputDirName.replace("-", "/");
		String outputDirectoryPath = baseOutputPath + Path.SEPARATOR + productType + Path.SEPARATOR + outputDirName + Path.SEPARATOR;
		LOG.info("Output Directory Path is " + outputDirectoryPath);
		return outputDirectoryPath;
	}


	private void readCommandLineArgs(String[] args) {
		jobName = args[0];
		tableName = args[1];
		baseInputPath = args[2];
		baseOutputPath = args[3];
		decompressedDirPath = args[4];
	    s3BucketLocation = args[5];
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.out.println("usage: <jobName> <tablename> <input> <output> <decom_dir> <s3bucket> [oozie]");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new AvroSerializerDriver(), args);
		System.exit(res);
	}

}
