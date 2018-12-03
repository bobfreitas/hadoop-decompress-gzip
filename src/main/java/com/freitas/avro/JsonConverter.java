package com.freitas.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class JsonConverter<T extends GenericRecord> {

	private final ObjectMapper mapper = new ObjectMapper();
	private final Schema baseSchema;

	public JsonConverter(Schema schema) {
		this.baseSchema = schema;
	}

	@SuppressWarnings("unchecked")
	public T convert(String json) throws IOException {
		try {
			return (T) convert(mapper.readValue(json, Map.class), baseSchema);
		}
		catch (IOException e) {
			throw new IOException("Failed to parse as Json: "+ json + "\n\n" + e.getMessage());
		}
	}

	@SuppressWarnings({ "unchecked", "deprecation", "rawtypes" })
	private T convert(Map<String, Object> raw, Schema schema)
			throws IOException {
		GenericRecord result = new GenericData.Record(schema);
		Set<String> usedFields = Sets.newHashSet();
		Set<String> missingFields = Sets.newHashSet();
		for (Schema.Field f : schema.getFields()) {
			String name = f.name();
			Object rawValue = raw.get(name);
			if (rawValue != null) {
				result.put(f.pos(), typeConvert(rawValue, name, f.schema()));
				usedFields.add(name);
			} else {
				missingFields.add(name);
				JsonNode defaultValue = f.defaultValue();
				if (defaultValue == null || defaultValue.isNull()) {
					if (isNullableSchema(f.schema())) {
						result.put(f.pos(), null);
					} else {
						throw new IllegalArgumentException(
								"No default value provided for non-nullable field: " + f.name());
					}
				} else {
					Schema fieldSchema = f.schema();
					if (isNullableSchema(fieldSchema)) {
						fieldSchema = getNonNull(fieldSchema);
					}
					Object value = null;
					switch (fieldSchema.getType()) {
					case BOOLEAN:
						value = defaultValue.getValueAsBoolean();
						break;
					case DOUBLE:
						value = defaultValue.getValueAsDouble();
						break;
					case FLOAT:
						value = (float) defaultValue.getValueAsDouble();
						break;
					case INT:
						value = defaultValue.getValueAsInt();
						break;
					case LONG:
						value = defaultValue.getValueAsLong();
						break;
					case STRING:
						value = defaultValue.getValueAsText();
						break;
					case MAP:
						Map<String, Object> fieldMap = mapper.readValue(
								defaultValue.getValueAsText(), Map.class);
						Map<String, Object> mvalue = Maps.newHashMap();
						for (Map.Entry<String, Object> e : fieldMap.entrySet()) {
							mvalue.put(e.getKey(),
									typeConvert(e.getValue(), name, fieldSchema.getValueType()));
						}
						value = mvalue;
						break;
					case ARRAY:
						List fieldArray = mapper.readValue(
								defaultValue.getValueAsText(), List.class);
						List lvalue = Lists.newArrayList();
						for (Object elem : fieldArray) {
							lvalue.add(typeConvert(elem, name, fieldSchema.getElementType()));
						}
						value = lvalue;
						break;
					case RECORD:
						Map<String, Object> fieldRec = mapper.readValue(
								defaultValue.getValueAsText(), Map.class);
						value = convert(fieldRec, fieldSchema);
						break;
					default:
						throw new IllegalArgumentException(
								"JsonConverter cannot handle type: " + fieldSchema.getType());
					}
					result.put(f.pos(), value);
				}
			}
		}
		return (T) result;
	}



	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object typeConvert(Object value, String name, Schema schema) throws IOException {
		if (isNullableSchema(schema)) {
			if (value == null) {
				return null;
			} else {
				schema = getNonNull(schema);
			}
		} else if (value == null) {
			// Always fail on null for non-nullable schemas
			throw new JsonConversionException(value, name, schema);
		}

		switch (schema.getType()) {
		case BOOLEAN:
			if (value instanceof Boolean) {
				return (Boolean) value;
			} else if (value instanceof String) {
				return Boolean.valueOf((String) value);
			} else if (value instanceof Number) {
				return ((Number) value).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
			}
			break;
		case DOUBLE:
			if (value instanceof Number) {
				return ((Number) value).doubleValue();
			} else if (value instanceof String) {
				return Double.valueOf((String) value);
			}
			break;
		case FLOAT:
			if (value instanceof Number) {
				return ((Number) value).floatValue();
			} else if (value instanceof String) {
				return Float.valueOf((String) value);
			}
			break;
		case INT:
			if (value instanceof Number) {
				return ((Number) value).intValue();
			} else if (value instanceof String) {
				return Integer.valueOf((String) value);
			}
			break;
		case LONG:
			if (value instanceof Number) {
				return ((Number) value).longValue();
			} else if (value instanceof String) {
				return Long.valueOf((String) value);
			}
			break;
		case STRING:
			return value.toString();
		case ENUM:
			try {
				Class<Enum> enumType = (Class<Enum>) Class.forName(schema.getFullName());
				return Enum.valueOf(enumType, value.toString());
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		case RECORD:
			return convert((Map<String, Object>) value, schema);
		case ARRAY:
			Schema elementSchema = schema.getElementType();
			List listRes = new ArrayList();
			for (Object v : (List) value) {
				listRes.add(typeConvert(v, name, elementSchema));
			}
			return listRes;
		case MAP:
			Schema valueSchema = schema.getValueType();
			Map<String, Object> mapRes = new HashMap<String, Object>();
			for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
				mapRes.put(v.getKey(), typeConvert(v.getValue(), name, valueSchema));
			}
			return mapRes;
		default:
			throw new IllegalArgumentException(
					"JsonConverter cannot handle type: " + schema.getType());
		}
		throw new JsonConversionException(value, name, schema);
	}

	private Schema getNonNull(Schema schema) {
		List<Schema> types = schema.getTypes();
		return types.get(0).getType().equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
	}

	private boolean isNullableSchema(Schema schema) {
		return schema.getType().equals(Schema.Type.UNION) &&
				schema.getTypes().size() == 2 &&
				(schema.getTypes().get(0).getType().equals(Schema.Type.NULL) ||
						schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
	}

	@SuppressWarnings("serial")
	private static class JsonConversionException extends RuntimeException {
		private Object value;
		private String fieldName;
		private Schema schema;

		public JsonConversionException(Object value, String fieldName, Schema schema) {
			this.value = value;
			this.fieldName = fieldName;
			this.schema = schema;
		}

		@Override
		public String toString() {
			return String.format("Type conversion error for field %s, %s for %s",
					fieldName, value, schema);
		}
	}
}
