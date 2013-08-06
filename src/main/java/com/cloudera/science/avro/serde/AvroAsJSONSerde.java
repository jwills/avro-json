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
import java.util.List;
import java.util.Properties;

import com.cloudera.science.avro.common.QualityReporter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.cloudera.science.avro.common.JsonConverter;
import com.cloudera.science.avro.common.SchemaLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class AvroAsJSONSerde implements SerDe {
  // Follow convention of AvroSerDe
  public static final String SCHEMA_LITERAL = "avro.schema.literal";
  public static final String SCHEMA_URL = "avro.schema.url";
  
  private Schema schema;
  private JsonConverter converter;
  private AvroGenericRecordWritable agrw = new AvroGenericRecordWritable();
  private List<Object> row = Lists.newArrayList();
  private ObjectInspector oi;
  
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    SchemaLoader loader = new SchemaLoader(conf);
    try {
      this.schema = loader.load(tbl.getProperty(SCHEMA_LITERAL), tbl.getProperty(SCHEMA_URL));
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    this.converter = new JsonConverter(schema, new QualityReporter());
    row.add("");
    
    String colName = tbl.getProperty(Constants.LIST_COLUMNS);
    if (colName == null || colName.isEmpty()) {
      colName = "json"; // use a default
    }
    
    this.oi = ObjectInspectorFactory.getStandardStructObjectInspector(
        ImmutableList.of(colName),
        ImmutableList.<ObjectInspector>of(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    row.set(0, ((AvroGenericRecordWritable) blob).getRecord().toString());
    return row;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return oi;
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
    StructObjectInspector soi = (StructObjectInspector) oi;
    List<Object> data = soi.getStructFieldsDataAsList(soi);
    StringObjectInspector foi = (StringObjectInspector) 
        soi.getAllStructFieldRefs().get(0).getFieldObjectInspector();
      try {
        agrw.setRecord(converter.convert(foi.getPrimitiveJavaObject(data.get(0))));
      } catch (IOException e) {
      throw new SerDeException(e);
    }
    return agrw;
  }
}
