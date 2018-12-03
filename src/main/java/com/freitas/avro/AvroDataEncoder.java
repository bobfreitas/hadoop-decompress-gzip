package com.freitas.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvroDataEncoder {

	public GenericRecord encodeAvroRecord(String sanitizedJjson, Schema schema) throws Exception {
		try {
			@SuppressWarnings("rawtypes")
			JsonConverter converter = new JsonConverter(schema);
			GenericRecord avro = converter.convert(sanitizedJjson);
			return avro;
		} catch (Exception e) {
			throw new IOException("Not able to encode avro record. Sanitized json: " + sanitizedJjson, e);
		}
	}

	public void decodeAvro(InputStream is, OutputStream os) throws Exception {
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

		try (
				DataFileStream<GenericRecord> dataStream = new DataFileStream<>(is, datumReader);
				PrintStream print = new PrintStream(os)) {
			while (dataStream.hasNext()) {
				GenericRecord record = dataStream.next();

				print.print(record);
				print.print("\n");
			}
		} catch (Exception e) {
			throw new IOException("Not able to decode avro record", e);
		}
	}

	public Schema parseSchema(String schema) throws Exception {
		try {
			Schema.Parser parser = new Schema.Parser();
			return parser.parse(schema);
		} catch (Exception e) {
			throw new IOException("Not able to parse avro schema: " + schema, e);
		}
	}

}
