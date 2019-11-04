package com.ververica.field.dynamicrules.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * The accumulator is used to keep a running sum and a count. The {@code getResult} method computes
 * the average.
 */
public class AverageAggregate implements AggregateFunction<Long, Tuple2<Long, Long>, String> {
  @Override
  public Tuple2<Long, Long> createAccumulator() {
    return new Tuple2<>(0L, 0L);
  }

  @Override
  public Tuple2<Long, Long> add(Long value, Tuple2<Long, Long> accumulator) {
    return new Tuple2<>(accumulator.f0 + value, accumulator.f1 + 1L);
  }

  @Override
  public String getResult(Tuple2<Long, Long> accumulator) {
    return String.valueOf(((double) accumulator.f0) / accumulator.f1);
  }

  @Override
  public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
    return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
  }
}
