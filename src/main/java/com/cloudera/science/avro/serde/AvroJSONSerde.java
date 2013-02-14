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

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AvroJSONSerde implements SerDe {

  private AvroSerDe delegate;
  
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    delegate = new AvroSerDe();
    delegate.initialize(conf, tbl);
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    return delegate.deserialize(blob);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return delegate.getObjectInspector();
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
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    Writable w = delegate.serialize(obj, objInspector);
    return new Text(((AvroGenericRecordWritable) w).getRecord().toString());
  }
}
