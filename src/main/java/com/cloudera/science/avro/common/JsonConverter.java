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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class JsonConverter<T extends GenericRecord> {
  private static final Log LOG = LogFactory.getLog(JsonConverter.class);
  private static final Set<Schema.Type> SUPPORTED_TYPES = ImmutableSet.of(
          Schema.Type.RECORD, Schema.Type.ARRAY, Schema.Type.MAP,
          Schema.Type.INT, Schema.Type.LONG, Schema.Type.BOOLEAN,
          Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING,
          Schema.Type.ENUM);

  private final Class<T> typeClass;
  private final QualityReporter reporter;
  private final ObjectMapper mapper = new ObjectMapper();
  private final Schema baseSchema;
  private int logMessageCounter = 0;
  private long ignoredFieldsCount = 0;
  private long usedFieldsCount = 0;
  private long missedFieldsCount = 0;

  public JsonConverter(Schema schema, QualityReporter reporter) {
    this(null, schema, reporter);
  }

  public JsonConverter(Class<T> clazz, Schema schema, QualityReporter reporter) {
    this.typeClass = clazz;
    this.baseSchema = checkSchema(schema, true);
    this.reporter = reporter;
    assert SchemaBuilder.builder().booleanBuilder().endBoolean() != null;
  }

  private Schema checkSchema(Schema schema, boolean mustBeRecord) {
    if (!mustBeRecord) {
      if (!SUPPORTED_TYPES.contains(schema.getType())) {
        throw new IllegalArgumentException("Unsupported type: " + schema.getType());
      }
      if (schema.getType() != Schema.Type.RECORD) {
        return schema;
      }
    }
    for (Schema.Field f : schema.getFields()) {
      Schema fs = f.schema();
      if (isNullableSchema(fs)) {
        fs = getNonNull(fs);
      }
      if(fs == null) {
        throw new IllegalArgumentException(String.format(
                "Schema is nul for field '%s'",  f.name()));
      }
      Schema.Type st = fs.getType();
      if (st == null || !SUPPORTED_TYPES.contains(st)) {
        throw new IllegalArgumentException(String.format(
                "Unsupported type  for field '%s'",  f.name()));
      }
      switch (st) {
        case RECORD:
          checkSchema(fs, true);
          break;
        case MAP:
          checkSchema(fs.getValueType(), false);
          break;
        case ARRAY:
          checkSchema(fs.getElementType(), false);
        default:
          break; // No need to check primitives
      }
    }
    return schema;
  }

  public T convert(String json) throws IOException {
    try {
      return  convert(json, null);
    }
    catch (IOException e) {
      throw new IOException("Failed to parse as Json: "+ json + "\n\n" + e.getMessage());
    }
  }

  /**
   *
   * @param json
   * @param rightSchemaName
   * @return T
   * @throws IOException
   */
  public T convert(String json, String rightSchemaName) throws IOException {
    try {
      return convert(mapper.readValue(json, Map.class), baseSchema, rightSchemaName);
    }
    catch (IOException e) {
      throw new IOException("Failed to parse as Json: "+ json + "\n\n" + e.getMessage(), e);
    }
  }


  private T convert(Map<String, Object> raw, Schema schema, String rightSchemaName)
          throws IOException {

    GenericRecord result;
    try {
      result = typeClass == null ? new GenericData.Record(schema) : (GenericRecord) ReflectionUtils.newInstance( Class.forName(schema.getFullName()), null);
    } catch (ClassNotFoundException e) {
      throw new IOException(String.format("Class %s is not exists in classpath", schema.getFullName()),e);
    }
    Set<String> usedFields = Sets.newHashSet();
    Set<String> missingFields = Sets.newHashSet();
    for (Schema.Field f : schema.getFields()) {
      String name = f.name();

      if (raw.containsKey(name) ) {
        Object rawValue = raw.get(name);
        result.put(f.pos(), typeConvert(rawValue, name, f.schema(), rightSchemaName));
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
          if (fieldSchema == null) {
            throw new IllegalArgumentException("The schema is null for field " + f);

          }
          if(fieldSchema.getType() == null){
            throw new IllegalArgumentException(
                    "JsonConverter cannot handle type: " + fieldSchema.getFullName());
          }
          switch (fieldSchema.getType()) {
            case BOOLEAN:
              value = defaultValue.getBooleanValue();
              break;
            case DOUBLE:
              value = defaultValue.getDoubleValue();
              break;
            case FLOAT:
              value = (float) defaultValue.getDoubleValue();
              break;
            case INT:
              value = defaultValue.getIntValue();
              break;
            case LONG:
              value = defaultValue.getLongValue();
              break;
            case STRING:
              value = defaultValue.getTextValue();
              break;
            case MAP:
              Map<String, Object> fieldMap = mapper.readValue(
                      defaultValue.getTextValue(), Map.class);
              Map<String, Object> mvalue = Maps.newHashMap();
              for (Map.Entry<String, Object> e : fieldMap.entrySet()) {
                mvalue.put(e.getKey(),
                        typeConvert(e.getValue(), name, fieldSchema.getValueType(), rightSchemaName));
              }
              value = mvalue;
              break;
            case ARRAY:
              List fieldArray = mapper.readValue(
                      defaultValue.getTextValue(), List.class);
              List lvalue = Lists.newArrayList();
              for (Object elem : fieldArray) {
                lvalue.add(typeConvert(elem, name, fieldSchema.getElementType(), rightSchemaName));
              }
              value = lvalue;
              break;
            case RECORD:
              Map<String, Object> fieldRec = mapper.readValue(
                      defaultValue.getTextValue(), Map.class);
              value = convert(fieldRec, fieldSchema, rightSchemaName);
              break;
            case ENUM:

              if(typeClass == null || typeClass ==GenericRecord.class) {
                value = new GenericData.EnumSymbol(schema, defaultValue.getTextValue());
              } else {
                Class<?> clz;
                try {
                  clz = Class.forName(fieldSchema.getFullName());
                } catch (ClassNotFoundException e) {
                  throw new IllegalArgumentException(
                          "JsonConverter cannot handle type: " + fieldSchema.getFullName());
                }
                Object[] enumConstants = clz.getEnumConstants();
                for(Object enumItem : enumConstants){
                  if(enumItem.toString().equals(defaultValue.getTextValue())){
                    value = enumItem;
                    break;
                  }
                }
                if(value == null){
                  throw new IllegalArgumentException(
                          "JsonConverter cannot handle type: " + fieldSchema.getFullName());
                }
              }

              break;
            default:
              throw new IllegalArgumentException(
                      "JsonConverter cannot handle type: " + fieldSchema.getType());
          }
          result.put(f.pos(), value);
        }
      }
    }

    if (usedFields.size() < raw.size()) {
      // Log a notification about unused fields
      if (logMessageCounter % 1000 == 0) {
        LOG.warn("Ignoring unused JSON fields: " + Sets.difference(raw.keySet(), usedFields));
      }
      logMessageCounter++;
      ignoredFieldsCount += (raw.size() - usedFields.size());
    }
    missedFieldsCount += missingFields.size();
    usedFieldsCount += usedFields.size();
    reporter.reportMissingFields(missingFields);

    return (T) result;
  }

  private Object typeConvert(Object value, String name, Schema schemax, String rightSchemaName) throws IOException {
    List<Schema> schemas = new ArrayList<Schema>();
    List<Object> values = new ArrayList<Object>();
    schemas.add(schemax);
    if (isNullableSchema(schemax)) {
      if (value == null) {
        return null;
      } else {
        schemas = getNonNullSchemas(schemax);
      }
    } else if (value == null) {
      // Always fail on null for non-nullable schemas
      throw new JsonConversionException(value, name, schemax);
    }

    if(schemas.size() == 1){
      values.add(convertBySchema(value, name, schemas.get(0), rightSchemaName));
    } else {
      values.add(convertByRightSchema(value, name, schemas, rightSchemaName));
    }

    if(values.size() == 1){
      return values.get(0);
    } else if(Map.class.isAssignableFrom(value.getClass())){
      Set<String> keys = ((Map<String, Object>)value).keySet();
      for(Object convertedValue : values){
        if(GenericData.Record.class.getName().equals(convertedValue.getClass().getName())) {
          List<Schema.Field> fields =((GenericData.Record) convertedValue).getSchema().getFields();
          if(keys.size() == fields.size()){

            boolean isTheSameFields = true;
            for(Schema.Field field : fields){
              if(!keys.contains(field.name())){
                isTheSameFields = false;
                break;
              }

            }
            if(isTheSameFields){
              return convertedValue;
            }
          }
        }
      }
    }
    throw new JsonConversionException(value, name, schemax);
  }

  private Object convertByRightSchema(Object value, String name, List<Schema> schemas, String rightSchemaName) throws IOException {
    Map<String, Long> mapMissingFieldsCount = new HashMap<String, Long>();
    Map<String, Object> mapConvertedObjects = new HashMap<String, Object>();
    Map<String, Long> mapIgnoredFieldsCount = new HashMap<String, Long>();
    Map<String, Long> mapUsedFieldsCount = new HashMap<String, Long>();
    for (Schema schema : schemas) {

      String schemaName = schema.getName();
      try {
        long globalMissingFieldsCounter = missedFieldsCount;
        long globalIgnoredFieldsCount = ignoredFieldsCount;
        long globalUsedFieldsCount = usedFieldsCount;
        mapConvertedObjects.put(schema.getName(), convertBySchema(value, name, schema, rightSchemaName));
        long missingFieldsCounterInConvert = missedFieldsCount - globalMissingFieldsCounter;
        long ignoredFieldsCounterInConvert = ignoredFieldsCount - globalIgnoredFieldsCount;
        long usedFieldsCounterInConvert = usedFieldsCount - globalUsedFieldsCount;

        mapMissingFieldsCount.put(schemaName, missingFieldsCounterInConvert);
        mapIgnoredFieldsCount.put(schemaName, ignoredFieldsCounterInConvert);
        mapUsedFieldsCount.put(schemaName, usedFieldsCounterInConvert);
      } catch (IllegalArgumentException ex) {
        //the schema is not suitable for value
      }
    }
    long maxUsedFields = Collections.max(mapUsedFieldsCount.values());

    for (String schemaName : mapUsedFieldsCount.keySet()) {
      Long usedCount = mapUsedFieldsCount.get(schemaName);
      if (usedCount == null || usedCount != maxUsedFields) {
        mapIgnoredFieldsCount.remove(schemaName);
        mapMissingFieldsCount.remove(schemaName);
      }
    }


    leaveMinValuesInMaps(mapIgnoredFieldsCount, mapMissingFieldsCount);
    leaveMinValuesInMaps(mapMissingFieldsCount,mapIgnoredFieldsCount);

    String schemaName = null;



    Set<String> commonSchemas = mapIgnoredFieldsCount.keySet();
    commonSchemas.retainAll(mapMissingFieldsCount.keySet());

    if(commonSchemas.size() == 1) {
      schemaName = commonSchemas.iterator().next();
    } else if(rightSchemaName != null && commonSchemas.contains(rightSchemaName)){
      schemaName = rightSchemaName;
    }
    if(schemaName!= null){
      return mapConvertedObjects.get(schemaName);
    } else {
      throw new JsonGenerationException("Several scemas are identified for object " + value +  ". It need to provide right schema between next " + commonSchemas);
    }


  }

  private void leaveMinValuesInMaps(Map<String, Long> mainMap, Map<String, Long> additionalMap){
    long minValue = Collections.min((mainMap.values()));
    Iterator<Map.Entry<String, Long>> iterator = mainMap.entrySet().iterator();
    while(iterator.hasNext()){
      Map.Entry<String, Long> entry = iterator.next();
      Long count = entry.getValue();
      String schemaName = entry.getKey();
      if (count == null || count != minValue) {
        additionalMap.remove(schemaName);
        iterator.remove();
      }
    }

  }

  private  Object convertBySchema(Object value, String name, Schema schema, String rightSchemaName) throws IOException {
    switch (schema.getType()) {
      case BOOLEAN:
        if (value instanceof Boolean) {
          return value;
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
        if(typeClass == null || typeClass == GenericRecord.class){
          return new GenericData.EnumSymbol(schema, value.toString());
        }
        try {
          Class<Enum> enumType = (Class<Enum>) Class.forName(schema.getFullName());
          return Enum.valueOf(enumType, value.toString());

        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }

      case RECORD:
        return convert((Map<String, Object>) value, schema, rightSchemaName);
      case ARRAY:
        Schema elementSchema = schema.getElementType();
        List listRes = new ArrayList();
        for (Object v : (List) value) {
          listRes.add(typeConvert(v, name, elementSchema, rightSchemaName));
        }
        return listRes;
      case MAP:
        Schema valueSchema = schema.getValueType();
        Map<String, Object> mapRes = new HashMap<String, Object>();
        for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
          mapRes.put(v.getKey(), typeConvert(v.getValue(), name, valueSchema, rightSchemaName));
        }
        return mapRes;
      default:
        throw new IllegalArgumentException(
                "JsonConverter cannot handle type: " + schema.getType());
    }
    throw new JsonConversionException(value, name, schema);
  }

  private boolean isNullableSchema(Schema schema) {
    return schema.getType().equals(Schema.Type.UNION) && schema.getTypes().size() > 1 && isContainsNullType(schema.getTypes());


  }

  private boolean isContainsNullType(List<Schema> schemas){
    for(Schema schema : schemas){
      if(schema != null && Schema.Type.NULL.equals(schema.getType())){
        return  true;
      }
    }
    return false;
  }

  private Schema getNonNull(Schema schema) {
    List<Schema> types = schema.getTypes();
    for(Schema type : types){
      if(schema != null && !Schema.Type.NULL.equals(type.getType())){
        return type;
      }
    }
    return null;
  }

  private List<Schema> getNonNullSchemas(Schema schema) {
    List<Schema> schemas = new ArrayList<Schema>();
    if(schema != null) {
      List<Schema> types = schema.getTypes();
      for (Schema type : types) {
        if (type != null && !Schema.Type.NULL.equals(type.getType())){
          schemas.add(type);
        }
      }
    }
    return schemas;
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
