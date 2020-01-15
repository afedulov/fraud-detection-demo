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

package com.ververica.field.dynamicrules.util;

import java.util.Arrays;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;

/**
 * A test harness for testing a {@link TwoInputStreamOperator}.
 *
 * <p>This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements and
 * watermarks can be retrieved. you are free to modify these.
 */
public class BroadcastStreamKeyedOperatorTestHarness<K, IN1, IN2, OUT>
    extends AbstractStreamOperatorTestHarness<OUT> {

  private final CoBroadcastWithKeyedOperator<K, IN1, IN2, OUT> twoInputOperator;

  public BroadcastStreamKeyedOperatorTestHarness(
      CoBroadcastWithKeyedOperator<K, IN1, IN2, OUT> operator,
      KeySelector<IN1, K> keySelector1,
      KeySelector<IN2, K> keySelector2,
      TypeInformation<K> keyType)
      throws Exception {
    this(operator, keySelector1, keySelector2, keyType, 1, 1, 0);
  }

  public BroadcastStreamKeyedOperatorTestHarness(
      CoBroadcastWithKeyedOperator<K, IN1, IN2, OUT> operator,
      KeySelector<IN1, K> keySelector1,
      KeySelector<IN2, K> keySelector2,
      TypeInformation<K> keyType,
      int maxParallelism,
      int numSubtasks,
      int subtaskIndex)
      throws Exception {
    super(operator, maxParallelism, numSubtasks, subtaskIndex);

    ClosureCleaner.clean(keySelector1, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector2, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    config.setStatePartitioner(0, keySelector1);
    config.setStatePartitioner(1, keySelector2);
    config.setStateKeySerializer(keyType.createSerializer(executionConfig));

    this.twoInputOperator = operator;
  }

  public void processElement1(StreamRecord<IN1> element) throws Exception {
    twoInputOperator.setKeyContextElement1(element);
    twoInputOperator.processElement1(element);
  }

  public void processElement2(StreamRecord<IN2> element) throws Exception {
    twoInputOperator.setKeyContextElement2(element);
    twoInputOperator.processElement2(element);
  }

  public void processWatermark1(Watermark mark) throws Exception {
    twoInputOperator.processWatermark1(mark);
  }

  public void processWatermark2(Watermark mark) throws Exception {
    twoInputOperator.processWatermark2(mark);
  }

  public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor)
      throws Exception {
    return twoInputOperator.getOperatorStateBackend().getBroadcastState(stateDescriptor);
  }

  public static <K, IN1, IN2, OUT>
      BroadcastStreamKeyedOperatorTestHarness<K, IN1, IN2, OUT> getInitializedTestHarness(
          final KeyedBroadcastProcessFunction<K, IN1, IN2, OUT> function,
          final KeySelector<IN1, K> keySelector1,
          final KeySelector<IN2, K> keySelector2,
          final TypeInformation<K> keyType,
          final MapStateDescriptor<?, ?>... descriptors)
          throws Exception {

    return getInitializedTestHarness(
        function, keySelector1, keySelector2, keyType, 1, 1, 0, descriptors);
  }

  public static <K, IN1, IN2, OUT>
      BroadcastStreamKeyedOperatorTestHarness<K, IN1, IN2, OUT> getInitializedTestHarness(
          final KeyedBroadcastProcessFunction<K, IN1, IN2, OUT> function,
          final KeySelector<IN1, K> keySelector1,
          final KeySelector<IN2, K> keySelector2,
          final TypeInformation<K> keyType,
          final int maxParallelism,
          final int numTasks,
          final int taskIdx,
          final MapStateDescriptor<?, ?>... descriptors)
          throws Exception {

    return getInitializedTestHarness(
        function,
        keySelector1,
        keySelector2,
        keyType,
        maxParallelism,
        numTasks,
        taskIdx,
        null,
        descriptors);
  }

  public static <K, IN1, IN2, OUT>
      BroadcastStreamKeyedOperatorTestHarness<K, IN1, IN2, OUT> getInitializedTestHarness(
          final KeyedBroadcastProcessFunction<K, IN1, IN2, OUT> function,
          final KeySelector<IN1, K> keySelector1,
          final KeySelector<IN2, K> keySelector2,
          final TypeInformation<K> keyType,
          final int maxParallelism,
          final int numTasks,
          final int taskIdx,
          final OperatorSubtaskState initState,
          final MapStateDescriptor<?, ?>... descriptors)
          throws Exception {

    BroadcastStreamKeyedOperatorTestHarness<K, IN1, IN2, OUT> testHarness =
        new BroadcastStreamKeyedOperatorTestHarness<>(
            new CoBroadcastWithKeyedOperator<>(
                Preconditions.checkNotNull(function), Arrays.asList(descriptors)),
            keySelector1,
            keySelector2,
            keyType,
            maxParallelism,
            numTasks,
            taskIdx);
    testHarness.setup();
    testHarness.initializeState(initState);
    testHarness.open();

    return testHarness;
  }

  public void watermark(long timestamp) throws Exception {
    processWatermark1(new Watermark(timestamp));
    processWatermark2(new Watermark(timestamp));
  }
}
