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
package com.cloudera.science.avro.common;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 */
public class SchemaLoader {
  
  private final Configuration conf;
  
  public SchemaLoader(Configuration conf) {
    this.conf = conf;
  }
  
  public Schema load(String schemaJson, String schemaUrl) throws IOException {
    Schema.Parser parser = new Schema.Parser();
    if (schemaJson != null && !"none".equals(schemaJson)) {
      return parser.parse(schemaJson);
    } else {
      if (schemaUrl == null || "none".equals(schemaUrl)) {
        throw new IllegalArgumentException(
            "Neither schemaJson nor schemaUrl specified to SchemaLoader");
      }
      
      if (schemaUrl.toLowerCase().startsWith("hdfs://")) {
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream input = null;
        try {
          input = fs.open(new Path(schemaUrl));
          return parser.parse(input);
        } finally {
          if (input != null) {
            input.close();
          }
        }
      } else {
        InputStream is = null;
        try {
          is = new URL(schemaUrl).openStream();
          return parser.parse(is);
        } finally {
          if (is != null) {
            is.close();
          }
        }
      }
    }
  }
}
