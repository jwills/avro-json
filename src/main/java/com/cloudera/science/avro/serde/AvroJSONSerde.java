/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.science.avro.serde;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.cloudera.science.avro.common.JsonConverter;
import com.cloudera.science.avro.common.SchemaLoader;

public class AvroJSONSerde implements SerDe {
  public static final String SCHEMA_LITERAL = "avro.schema.literal";
  public static final String SCHEMA_URL = "avro.schema.url";
  
  private Schema schema;
  private JsonConverter converter;
  private AvroGenericRecordWritable agrw = new AvroGenericRecordWritable();
  
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    SchemaLoader loader = new SchemaLoader(conf);
    try {
      this.schema = loader.load(tbl.getProperty(SCHEMA_LITERAL), tbl.getProperty(SCHEMA_URL));
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    this.converter = new JsonConverter(schema);
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    return ((AvroGenericRecordWritable) blob).getRecord().toString();
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector oi) throws SerDeException {
    StringObjectInspector soi = (StringObjectInspector) oi;
    try {
      agrw.setRecord(converter.convert(soi.getPrimitiveJavaObject(obj)));
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    return agrw;
  }
}
