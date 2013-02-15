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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class AvroAsJSONRecordReader<T> implements RecordReader<Text, Text> {

  private FileReader<T> reader;
  private T datum;
  private long start;
  private long end;
  
  public AvroAsJSONRecordReader(Schema schema, JobConf job, FileSplit split) throws IOException {
    this(DataFileReader.openReader(new FsInput(split.getPath(), job),
        new GenericDatumReader<T>(schema)), split);
  }

  protected AvroAsJSONRecordReader(FileReader<T> reader, FileSplit split)
      throws IOException {
    this.reader = reader;
    reader.sync(split.getStart());
    this.start = reader.tell();
    this.end = split.getStart() + split.getLength();
  }
    
  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  @Override
  public long getPos() throws IOException {
    return reader.tell();
  }

  @Override
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getPos() - start) / (float)(end - start));
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public boolean next(Text key, Text value) throws IOException {
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
}
