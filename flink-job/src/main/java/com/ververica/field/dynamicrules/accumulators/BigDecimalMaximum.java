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
 * An accumulator that finds the maximum {@code BigDecimal} value.
 *
 * <p>Supports numbers greater than Double.MIN_VALUE.
 */
@PublicEvolving
public class BigDecimalMaximum implements SimpleAccumulator<BigDecimal> {

  private static final long serialVersionUID = 1L;

  private BigDecimal max = BigDecimal.valueOf(Double.MIN_VALUE);

  private final BigDecimal limit = BigDecimal.valueOf(Double.MIN_VALUE);

  public BigDecimalMaximum() {}

  public BigDecimalMaximum(BigDecimal value) {
    this.max = value;
  }

  // ------------------------------------------------------------------------
  //  Accumulator
  // ------------------------------------------------------------------------

  @Override
  public void add(BigDecimal value) {
    if (value.compareTo(limit) < 0) {
      throw new IllegalArgumentException(
          "BigDecimalMaximum accumulator only supports values greater than Double.MIN_VALUE");
    }
    this.max = max.max(value);
  }

  @Override
  public BigDecimal getLocalValue() {
    return this.max;
  }

  @Override
  public void merge(Accumulator<BigDecimal, BigDecimal> other) {
    this.max = max.max(other.getLocalValue());
  }

  @Override
  public void resetLocal() {
    this.max = BigDecimal.valueOf(Double.MIN_VALUE);
  }

  @Override
  public BigDecimalMaximum clone() {
    BigDecimalMaximum clone = new BigDecimalMaximum();
    clone.max = this.max;
    return clone;
  }

  // ------------------------------------------------------------------------
  //  Utilities
  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "BigDecimal " + this.max;
  }
}
