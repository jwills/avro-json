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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.cloudera.science.avro.common.JsonConverter;
import com.cloudera.science.avro.common.SchemaLoader;

/**
 *
 */
public class AvroAsJSONOutputFormat extends FileOutputFormat<Text, Text> {
  public static final String SCHEMA_LITERAL = "output.schema.literal";
  public static final String SCHEMA_URL = "output.schema.url";
  public static final String READ_KEY = "output.read.key";
  
  private Schema schema;
  private JsonConverter converter;
  private boolean readKey = true;
  
  @Override
  public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) throws IOException {
    if (schema == null) {
      SchemaLoader loader = new SchemaLoader(job);
      this.schema = loader.load(job.get(SCHEMA_LITERAL), job.get(SCHEMA_URL));
      this.converter = new JsonConverter(schema);
      this.readKey = job.getBoolean(READ_KEY, true);
    }
    
    DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(
        new GenericDatumWriter<GenericRecord>(schema));
    if (getCompressOutput(job)) {
      int level = job.getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, AvroOutputFormat.DEFAULT_DEFLATE_LEVEL);
      String codecName = job.get(AvroJob.CONF_OUTPUT_CODEC, 
          org.apache.avro.file.DataFileConstants.DEFLATE_CODEC);
      CodecFactory codec = codecName.equals(DataFileConstants.DEFLATE_CODEC)
          ? CodecFactory.deflateCodec(level)
          : CodecFactory.fromString(codecName);
      writer.setCodec(codec);
    }
    writer.setSyncInterval(job.getInt(AvroOutputFormat.SYNC_INTERVAL_KEY,
        DataFileConstants.DEFAULT_SYNC_INTERVAL));
    
    Path path = FileOutputFormat.getTaskOutputPath(job, name + AvroOutputFormat.EXT);
    writer.create(schema, path.getFileSystem(job).create(path));
    
    return new AvroAsJSONRecordWriter(writer, converter, readKey);
  }
}
