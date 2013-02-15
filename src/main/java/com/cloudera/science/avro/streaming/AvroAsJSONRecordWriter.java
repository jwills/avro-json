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
package com.cloudera.science.avro.streaming;

import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.cloudera.science.avro.common.JsonConverter;

public class AvroAsJSONRecordWriter implements RecordWriter<Text, Text> {

  private final DataFileWriter<GenericRecord> writer;
  private final JsonConverter converter;
  private final boolean readKey;
  
  public AvroAsJSONRecordWriter(DataFileWriter<GenericRecord> writer, JsonConverter converter, boolean readKey) {
    this.writer = writer;
    this.converter = converter;
    this.readKey = readKey;
  }
  
  @Override
  public void write(Text key, Text value) throws IOException {
    writer.append(converter.convert(readKey ? key.toString() : value.toString()));
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    writer.close();
  }
}
