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
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AvroAsJSONRecordReader<T> extends RecordReader<Text, Text> {

  private final Schema schema;
  private final Text key = new Text();
  private final Text value = new Text();
  
  private FileReader<T> reader;
  private T datum;
  private long start;
  private long end;
  
  public AvroAsJSONRecordReader(Schema schema) {
    this.schema = schema;
  }
  
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    FileSplit fs = (FileSplit) split;
    this.reader = DataFileReader.openReader(
        new FsInput(fs.getPath(), context.getConfiguration()),
        new GenericDatumReader<T>(schema));
    reader.sync(fs.getStart());
    this.start = reader.tell();
    this.end = fs.getStart() + fs.getLength();
    key.clear(); // the value is never written
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!reader.hasNext() || reader.pastSync(end)) {
      return false;
    }
    datum = reader.next(datum);
    if (datum instanceof ByteBuffer) {
      ByteBuffer b = (ByteBuffer) datum;
      if (b.hasArray()) {
        int offset = b.arrayOffset();
        int start = b.position();
        int length = b.remaining();
        key.set(b.array(), offset + start, offset + start + length);
      } else {
        byte[] bytes = new byte[b.remaining()];
        b.duplicate().get(bytes);
        key.set(bytes);
      }
    } else {
      key.set(datum.toString());
    }
    return true;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (reader.tell() - start) / (float)(end - start));
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
