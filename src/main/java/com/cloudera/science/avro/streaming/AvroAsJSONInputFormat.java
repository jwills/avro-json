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

import org.apache.avro.Schema;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.cloudera.science.avro.common.SchemaLoader;

/**
 *
 */
public class AvroAsJSONInputFormat<T> extends FileInputFormat<Text, Text> {
  public static final String SCHEMA_LITERAL = "input.schema.literal";
  public static final String SCHEMA_URL = "input.schema.url";
  private static final SchemaLoader SCHEMA_LOADER = new SchemaLoader(SCHEMA_LITERAL, SCHEMA_URL);
  
  private Schema schema;
  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (schema == null) {
      schema = SCHEMA_LOADER.load(context.getConfiguration());
    }
    return new AvroAsJSONRecordReader<T>(schema);
  }
}
