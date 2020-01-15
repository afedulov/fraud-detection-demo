/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.field.dynamicrules.accumulators;

import java.math.BigDecimal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/**
 * An accumulator that finds the minimum {@code BigDecimal} value.
 *
 * <p>Supports numbers less than Double.MAX_VALUE.
 */
@PublicEvolving
public class BigDecimalMinimum implements SimpleAccumulator<BigDecimal> {

  private static final long serialVersionUID = 1L;

  private BigDecimal min = BigDecimal.valueOf(Double.MAX_VALUE);

  private final BigDecimal limit = BigDecimal.valueOf(Double.MAX_VALUE);

  public BigDecimalMinimum() {}

  public BigDecimalMinimum(BigDecimal value) {
    this.min = value;
  }

  // ------------------------------------------------------------------------
  //  Accumulator
  // ------------------------------------------------------------------------

  @Override
  public void add(BigDecimal value) {
    if (value.compareTo(limit) > 0) {
      throw new IllegalArgumentException(
          "BigDecimalMinimum accumulator only supports values less than Double.MAX_VALUE");
    }
    this.min = min.min(value);
  }

  @Override
  public BigDecimal getLocalValue() {
    return this.min;
  }

  @Override
  public void merge(Accumulator<BigDecimal, BigDecimal> other) {
    this.min = min.min(other.getLocalValue());
  }

  @Override
  public void resetLocal() {
    this.min = BigDecimal.valueOf(Double.MAX_VALUE);
  }

  @Override
  public BigDecimalMinimum clone() {
    BigDecimalMinimum clone = new BigDecimalMinimum();
    clone.min = this.min;
    return clone;
  }

  // ------------------------------------------------------------------------
  //  Utilities
  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "BigDecimal " + this.min;
  }
}
