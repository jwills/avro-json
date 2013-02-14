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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.science.avro.common.JsonConverter;

/**
 *
 */
public class AvroAsJSONOutputFormat<T> extends FileOutputFormat<Text, Text> {
  private JsonConverter converter;
  private boolean readKey = true;
  
  @Override
  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    if (converter == null) {
      // fetch schema
    }
    DataFileWriter<GenericRecord> writer = null;
    return new AvroAsJSONRecordWriter<T>(writer, converter, readKey);
  }
}
