/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.science.avro.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 */
public class JsonConverter {
  private final ObjectMapper mapper = new ObjectMapper();
  private final Schema baseSchema;
  
  public JsonConverter(Schema schema) {
    this.baseSchema = schema;
  }
  
  public GenericRecord convert(String json) throws IOException {
    return convert(mapper.readValue(json, Map.class), baseSchema);
  }
  
  private GenericRecord convert(Map<String, Object> raw, Schema schema) {
    GenericRecord result = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      String name = f.name();
      if (raw.containsKey(name)) {
        result.put(f.name(), typeConvert(raw.get(name), name, f.schema()));
      }
    }
    return result;
  }
  
  private Object typeConvert(Object value, String name, Schema schema) {
    if (isNullSchema(schema)) {
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
  
  private boolean isNullSchema(Schema schema) {
    return schema.getType().equals(Schema.Type.UNION) &&
        schema.getTypes().size() == 2 &&
        (schema.getTypes().get(0).getType().equals(Schema.Type.NULL) ||
         schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
  }
  
  private Schema getNonNull(Schema schema) {
    List<Schema> types = schema.getTypes();
    return types.get(0).getType().equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
  }
  
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
