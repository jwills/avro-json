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
import java.util.List;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.cloudera.science.avro.common.SchemaLoader;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 *
 */
public class AvroAsJSONInputFormat extends FileInputFormat<Text, Text> {
  public static final String SCHEMA_LITERAL = "input.schema.literal";
  public static final String SCHEMA_URL = "input.schema.url";
  
  private List<Schema> schemas;
  private String[] inputPaths;


  protected Schema getSplitSchema(InputSplit split, JobConf job) throws IOException {
    if (schemas == null) {
      loadSchemas(job);
    }
    FileSplit fs = (FileSplit) split;
    Schema schema = null;
    if (schemas.size() == 1) {
      schema = schemas.get(0);
    } else {
      // Need to figure out which schema we're loading
      String current = fs.getPath().toString();
      int index = -1;
      int bestMatchLength = -1;
      for (int i = 0; i < inputPaths.length; i++) {
        int match = Strings.commonPrefix(current, inputPaths[i]).length();
        if (match > bestMatchLength) {
          bestMatchLength = match;
          index = i;
        }
      }
      schema = schemas.get(index);
    }
    return schema;
  }

  @Override
  public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    Schema schema = getSplitSchema(split, job);
    FileSplit fs = (FileSplit) split;
    return new AvroAsJSONRecordReader(schema, job, fs);
  }
  
  private void loadSchemas(JobConf job) throws IOException {
    this.schemas = Lists.newArrayList();
    SchemaLoader loader = new SchemaLoader(job);
    String schemaLiteral = job.get(SCHEMA_LITERAL);
    if (schemaLiteral != null) {
      schemas.add(loader.loadLiteral(schemaLiteral));
      return;
    } else {
      String[] schemaUrls = job.getStrings(SCHEMA_URL);
      if (schemaUrls != null) {
        HashMap<String, Schema> hmap = new HashMap<String, Schema>();
        for (String schemaUrl : schemaUrls) {
          if (!hmap.containsKey(schemaUrl)) {
            hmap.put(schemaUrl, loader.loadFromUrl(schemaUrl));
          }
          schemas.add(hmap.get(schemaUrl));
        }
        if (schemaUrls.length > 1) {
          // Need to track input paths
          Path[] inputs = FileInputFormat.getInputPaths(job);
          if (inputs.length != schemaUrls.length) {
            throw new IllegalArgumentException(String.format(
                "Number of input paths (%d) does not match number of schema URLs (%d)",
                inputs.length, schemaUrls.length));
          }
          this.inputPaths = new String[inputs.length];
          for (int i = 0; i < inputs.length; i++) {
            inputPaths[i] = inputs[i].toString();
          }
        }
      } else {
        throw new IllegalArgumentException("No schema information provided");
      }
    }
  }
}
