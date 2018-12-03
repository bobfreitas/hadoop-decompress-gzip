package com.freitas.mapreduce;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.freitas.avro.AvroDataEncoder;
import com.freitas.commons.SerializerConstants;

public class SerializerAvroMapper extends Mapper<Object, Text, AvroKey<GenericRecord>, NullWritable> {
	
	private static final Logger LOG = Logger.getLogger(SerializerAvroMapper.class);
	
	private org.apache.avro.Schema avroSchema = null;
	private AvroDataEncoder avroDataEncoder = null;
	
	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		avroDataEncoder = new AvroDataEncoder();
		String avroSchemaContents = conf.get(SerializerConstants.AVRO_SCHEMA);
		try {
			avroSchema = avroDataEncoder.parseSchema(avroSchemaContents);
		} catch (Exception e) {
			String msg = "Unable to read Avro schema: " + avroSchemaContents;
			LOG.error(msg, e);
			throw new IOException(msg);
		}
	}

	@Override
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		try {
			if(value != null) {
				GenericRecord convertedAvroRecord = avroDataEncoder.encodeAvroRecord(value.toString(), avroSchema);
				context.write(new AvroKey<GenericRecord>(convertedAvroRecord), NullWritable.get());
			} else {
				context.getCounter(AvroSerializerDriver.COUNTERS.EMPTY_JSON).increment(1L);
				LOG.error("Empty record found ");
			}
		}
		catch(Exception e) {
			LOG.error("Parser exception for value: " + value.toString(), e);
			context.getCounter(AvroSerializerDriver.COUNTERS.PARSE_FAILED).increment(1L);
		}
	}

}
