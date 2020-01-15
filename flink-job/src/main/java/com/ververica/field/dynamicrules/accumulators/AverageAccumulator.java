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
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/**
 * An accumulator that computes the average value. Input can be {@code long}, {@code integer}, or
 * {@code double} and the result is {@code double}.
 */
@Public
public class AverageAccumulator implements SimpleAccumulator<BigDecimal> {

  private static final long serialVersionUID = 1L;

  private long count;

  private BigDecimal sum;

  @Override
  public void add(BigDecimal value) {
    this.count++;
    this.sum = sum.add(value);
  }

  @Override
  public BigDecimal getLocalValue() {
    if (this.count == 0) {
      return BigDecimal.ZERO;
    }
    return this.sum.divide(new BigDecimal(count));
  }

  @Override
  public void resetLocal() {
    this.count = 0;
    this.sum = BigDecimal.ZERO;
  }

  @Override
  public void merge(Accumulator<BigDecimal, BigDecimal> other) {
    if (other instanceof AverageAccumulator) {
      AverageAccumulator avg = (AverageAccumulator) other;
      this.count += avg.count;
      this.sum = sum.add(avg.sum);
    } else {
      throw new IllegalArgumentException("The merged accumulator must be AverageAccumulator.");
    }
  }

  @Override
  public AverageAccumulator clone() {
    AverageAccumulator average = new AverageAccumulator();
    average.count = this.count;
    average.sum = this.sum;
    return average;
  }

  @Override
  public String toString() {
    return "AverageAccumulator " + this.getLocalValue() + " for " + this.count + " elements";
  }
}
