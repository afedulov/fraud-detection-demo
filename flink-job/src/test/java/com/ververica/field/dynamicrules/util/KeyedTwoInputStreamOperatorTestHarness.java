package com.ververica.field.dynamicrules.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;

/**
 * Extension of {@link TwoInputStreamOperatorTestHarness} that allows the operator to get a {@link
 * KeyedStateBackend}.
 */
public class KeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT>
    extends TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> {

  public KeyedTwoInputStreamOperatorTestHarness(
      TwoInputStreamOperator<IN1, IN2, OUT> operator,
      KeySelector<IN1, K> keySelector1,
      KeySelector<IN2, K> keySelector2,
      TypeInformation<K> keyType,
      int maxParallelism,
      int numSubtasks,
      int subtaskIndex)
      throws Exception {
    super(operator, maxParallelism, numSubtasks, subtaskIndex);

    ClosureCleaner.clean(keySelector1, false);
    ClosureCleaner.clean(keySelector2, false);
    config.setStatePartitioner(0, keySelector1);
    config.setStatePartitioner(1, keySelector2);
    config.setStateKeySerializer(keyType.createSerializer(executionConfig));
  }

  public KeyedTwoInputStreamOperatorTestHarness(
      TwoInputStreamOperator<IN1, IN2, OUT> operator,
      final KeySelector<IN1, K> keySelector1,
      final KeySelector<IN2, K> keySelector2,
      TypeInformation<K> keyType)
      throws Exception {
    this(operator, keySelector1, keySelector2, keyType, 1, 1, 0);
  }

  public int numKeyedStateEntries() {
    AbstractStreamOperator<?> abstractStreamOperator = (AbstractStreamOperator<?>) operator;
    KeyedStateBackend<Object> keyedStateBackend = abstractStreamOperator.getKeyedStateBackend();
    if (keyedStateBackend instanceof HeapKeyedStateBackend) {
      return ((HeapKeyedStateBackend) keyedStateBackend).numKeyValueStateEntries();
    } else {
      throw new UnsupportedOperationException();
    }
  }
}
