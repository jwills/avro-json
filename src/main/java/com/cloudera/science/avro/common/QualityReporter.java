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

import org.apache.hadoop.mapred.Reporter;

import java.util.Collection;

/**
 * Track statistics about how often certain fields are missing
 */
public class QualityReporter {
  public static final String TOP_LEVEL_GROUP = "OVERALL";
  public static final String TL_COUNTER_RECORD_HAS_MISSING_FIELDS = "RECORD_WITH_MISSING_FIELDS";
  public static final String FIELD_MISSING_GROUP = "FIELD_MISSING";

  private final Reporter reporter;

  public QualityReporter() {
    this(null);
  }

  public QualityReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  protected void inc(String group, String counter) {
    if (reporter != null) {
      reporter.incrCounter(group, counter, 1L);
    }
  }

  public void reportMissingFields(Collection<String> missingFieldNames) {
    if (!missingFieldNames.isEmpty()) {
      inc(TOP_LEVEL_GROUP, TL_COUNTER_RECORD_HAS_MISSING_FIELDS);
      for (String missingFieldName : missingFieldNames) {
        inc(FIELD_MISSING_GROUP, missingFieldName);
      }
    }
  }
}
