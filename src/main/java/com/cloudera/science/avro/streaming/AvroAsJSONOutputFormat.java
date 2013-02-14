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
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.science.avro.common.JsonConverter;
import com.cloudera.science.avro.common.SchemaLoader;

/**
 *
 */
public class AvroAsJSONOutputFormat<T> extends FileOutputFormat<Text, Text> {
  public static final String SCHEMA_LITERAL = "output.schema.literal";
  public static final String SCHEMA_URL = "output.schema.url";
  public static final String READ_KEY = "output.read.key";
  
  private static final SchemaLoader SCHEMA_LOADER = new SchemaLoader(SCHEMA_LITERAL, SCHEMA_URL);

  private Schema schema;
  private JsonConverter converter;
  private boolean readKey = true;
  
  @Override
  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    if (schema == null) {
      this.schema = SCHEMA_LOADER.load(conf);
      this.converter = new JsonConverter(schema);
      this.readKey = conf.getBoolean(READ_KEY, true);
    }
    
    DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(
        new GenericDatumWriter<GenericRecord>(schema));
    if (getCompressOutput(job)) {
      int level = conf.getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, AvroOutputFormat.DEFAULT_DEFLATE_LEVEL);
      String codecName = conf.get(AvroJob.CONF_OUTPUT_CODEC, 
          org.apache.avro.file.DataFileConstants.DEFLATE_CODEC);
      CodecFactory codec = codecName.equals(DataFileConstants.DEFLATE_CODEC)
          ? CodecFactory.deflateCodec(level)
          : CodecFactory.fromString(codecName);
      writer.setCodec(codec);
    }
    writer.setSyncInterval(conf.getInt(AvroOutputFormat.SYNC_INTERVAL_KEY,
        DataFileConstants.DEFAULT_SYNC_INTERVAL));
    
    Path path = getDefaultWorkFile(job, AvroOutputFormat.EXT);
    writer.create(schema, path.getFileSystem(conf).create(path));
    
    return new AvroAsJSONRecordWriter<T>(writer, converter, readKey);
  }
}
