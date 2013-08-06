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

import com.google.common.collect.Maps;
import com.sun.tools.javac.util.Pair;

import java.util.Map;

public class MockQualityReporter extends QualityReporter {

  private Map<Pair<String, String>, Long> counts = Maps.newHashMap();

  public MockQualityReporter() {
    super(null);
  }

  public void reset() {
    counts.clear();
  }

  public long get(String group, String counter) {
    Long c = counts.get(Pair.of(group, counter));
    return c == null ? 0L : c;
  }

  @Override
  protected void inc(String group, String counter) {
    Pair<String, String> key = Pair.of(group, counter);
    Long c = counts.get(key);
    c = (c == null) ? 1L : 1L + c;
    counts.put(key, c);
  }

}
