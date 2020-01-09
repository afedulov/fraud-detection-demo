/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.field.dynamicrules.functions;

import static com.ververica.field.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;

import com.ververica.field.dynamicrules.Keyed;
import com.ververica.field.dynamicrules.KeysExtractor;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.Rule.ControlType;
import com.ververica.field.dynamicrules.Rule.RuleState;
import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import com.ververica.field.dynamicrules.Transaction;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class DynamicKeyFunction
    extends BroadcastProcessFunction<Transaction, Rule, Keyed<Transaction, String, Integer>> {

  private RuleCounterGuage ruleCounterGuage;

  @Override
  public void open(Configuration parameters) {
    ruleCounterGuage = new RuleCounterGuage();
    getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGuage);
  }

  @Override
  public void processElement(
      Transaction event, ReadOnlyContext ctx, Collector<Keyed<Transaction, String, Integer>> out)
      throws Exception {
    ReadOnlyBroadcastState<Integer, Rule> rulesState =
        ctx.getBroadcastState(Descriptors.keysDescriptor);
    forkEventForEachGroupingKey(event, rulesState, out);
  }

  private void forkEventForEachGroupingKey(
      Transaction event,
      ReadOnlyBroadcastState<Integer, Rule> rulesState,
      Collector<Keyed<Transaction, String, Integer>> out)
      throws Exception {
    int ruleCounter = 0;
    for (Map.Entry<Integer, Rule> entry : rulesState.immutableEntries()) {
      final Rule rule = entry.getValue();
      out.collect(
          new Keyed<>(
              event, KeysExtractor.getKey(rule.getGroupingKeyNames(), event), rule.getRuleId()));
      ruleCounter++;
    }
    ruleCounterGuage.setValue(ruleCounter);
  }

  @Override
  public void processBroadcastElement(
      Rule rule, Context ctx, Collector<Keyed<Transaction, String, Integer>> out) throws Exception {
    log.info("{}", rule);
    BroadcastState<Integer, Rule> broadcastState =
        ctx.getBroadcastState(Descriptors.keysDescriptor);
    handleRuleBroadcast(rule, broadcastState);
    if (rule.getRuleState() == RuleState.CONTROL) {
      handleControlCommand(rule.getControlType(), broadcastState);
    }
  }

  private void handleControlCommand(
      ControlType controlType, BroadcastState<Integer, Rule> rulesState) throws Exception {
    switch (controlType) {
      case DELETE_RULES_ALL:
        Iterator<Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
        while (entriesIterator.hasNext()) {
          Entry<Integer, Rule> ruleEntry = entriesIterator.next();
          rulesState.remove(ruleEntry.getKey());
          log.info("Removed Rule {}", ruleEntry.getValue());
        }
        break;
    }
  }

  private static class RuleCounterGuage implements Gauge<Integer> {

    private int value = 0;

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public Integer getValue() {
      return value;
    }
  }
}
