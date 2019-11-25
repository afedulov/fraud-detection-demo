package com.ververica.field.dynamicrules.functions;

import static com.ververica.field.dynamicrules.functions.ProcessingUtils.addToStateValuesSet;
import static com.ververica.field.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;

import com.ververica.field.dynamicrules.Alert;
import com.ververica.field.dynamicrules.FieldsExtractor;
import com.ververica.field.dynamicrules.Keyed;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.Rule.ControlType;
import com.ververica.field.dynamicrules.Rule.RuleState;
import com.ververica.field.dynamicrules.RuleHelper;
import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import com.ververica.field.dynamicrules.Transaction;
import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

// TODO: For a more generic implementation consider using a composite key instead of String
@Slf4j
public class DynamicRuleFunction
    extends KeyedBroadcastProcessFunction<
        String, Keyed<Transaction, String, Integer>, Rule, Alert> {

  private static final String COUNT = "COUNT_FLINK";
  private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

  public final int WIDEST_RULE_KEY = Integer.MIN_VALUE;
  public final int CLEAR_STATE_COMMAND_KEY = Integer.MIN_VALUE + 1;

  private transient MapState<Long, Set<Transaction>> windowState;
  private Meter alertMeter;

  private MapStateDescriptor<Long, Set<Transaction>> windowStateDescriptor =
      new MapStateDescriptor<>(
          "windowState",
          BasicTypeInfo.LONG_TYPE_INFO,
          TypeInformation.of(new TypeHint<Set<Transaction>>() {}));

  @Override
  public void open(Configuration parameters) {

    windowState = getRuntimeContext().getMapState(windowStateDescriptor);

    alertMeter = new MeterView(60);
    getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
  }

  @Override
  public void processElement(
      Keyed<Transaction, String, Integer> value, ReadOnlyContext ctx, Collector<Alert> out)
      throws Exception {

    long currentEventTime = value.getWrapped().getEventTime();

    /*
        Rule cleanupCommand =
            ctx.getBroadcastState(Descriptors.rulesDescriptor).get(CLEAR_STATE_COMMAND_KEY);

        if (cleanupCommand != null && cleanupCommand.getControlType() == CLEAR_STATE_ALL) {
          evictAllStateElements();
          log.info("Removed state for key: {}", value.getKey());
          return;
        }
    */

    addToStateValuesSet(windowState, currentEventTime, value.getWrapped());

    Rule rule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(value.getId());
    if (rule == null) {
      log.error("Rule with {} does not exist", value.getId());
    }

    long ingestionTime = value.getWrapped().getIngestionTimestamp();
    ctx.output(Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime);

    if (noRuleAvailable(rule)) return;

    if (rule.getRuleState() == Rule.RuleState.ACTIVE) {
      Long windowStartForEvent = rule.getWindowStartFor(currentEventTime);

      long cleanupTime = (currentEventTime / 1000) * 1000;
      ctx.timerService().registerEventTimeTimer(cleanupTime);

      SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
      for (Long stateEventTime : windowState.keys()) {
        if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
          aggregateValuesInState(stateEventTime, aggregator, rule);
        }
      }
      BigDecimal aggregateResult = aggregator.getLocalValue();
      boolean ruleResult = rule.apply(aggregateResult);

      ctx.output(
          Descriptors.demoSinkTag,
          "Rule "
              + rule.getRuleId()
              + " | "
              + value.getKey()
              + " : "
              + aggregateResult.toString()
              + " -> "
              + ruleResult);

      if (ruleResult) {
        if (COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
          evictAllStateElements();
        }
        alertMeter.markEvent();
        out.collect(
            new Alert<>(
                rule.getRuleId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
      }
    }
  }

  @Override
  public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out)
      throws Exception {
    log.info("{}", rule);
    BroadcastState<Integer, Rule> broadcastState =
        ctx.getBroadcastState(Descriptors.rulesDescriptor);
    handleRuleBroadcast(rule, broadcastState);
    updateWidestWindowRule(rule, broadcastState);
    if (rule.getRuleState() == RuleState.CONTROL) {
      handleControlCommand(rule, broadcastState, ctx);
    }
  }

  private void handleControlCommand(
      Rule command, BroadcastState<Integer, Rule> rulesState, Context ctx) throws Exception {
    ControlType controlType = command.getControlType();
    switch (controlType) {
      case EXPORT_RULES_CURRENT:
        for (Map.Entry<Integer, Rule> entry : rulesState.entries()) {
          ctx.output(Descriptors.currentRulesSinkTag, entry.getValue());
        }
        break;
      case CLEAR_STATE_ALL:
        //        rulesState.put(CLEAR_STATE_COMMAND_KEY, command);
        ctx.applyToKeyedState(windowStateDescriptor, (key, state) -> state.clear());
        break;
      case CLEAR_STATE_ALL_STOP:
        rulesState.remove(CLEAR_STATE_COMMAND_KEY);
        break;
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

  private boolean isStateValueInWindow(
      Long stateEventTime, Long windowStartForEvent, long currentEventTime) {
    return stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime;
  }

  private void aggregateValuesInState(
      Long stateEventTime, SimpleAccumulator<BigDecimal> aggregator, Rule rule) throws Exception {
    Set<Transaction> inWindow = windowState.get(stateEventTime);
    if (COUNT.equals(rule.getAggregateFieldName())
        || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
      for (Transaction event : inWindow) {
        aggregator.add(BigDecimal.ONE);
      }
    } else {
      for (Transaction event : inWindow) {
        BigDecimal aggregatedValue =
            FieldsExtractor.getBigDecimalByName(
                rule.getAggregateFieldName(),
                event); // Should be double or BigDecimal in the first place
        aggregator.add(aggregatedValue);
      }
    }
  }

  private boolean noRuleAvailable(Rule rule) {
    // This happens if the BroadcastState in this CoProcessFunction was updated before it was
    // updated in `DynamicKeyFunction`
    // TODO Maybe we should consider sending the whole Rule Object with each event to avoid this
    // race condition.
    if (rule == null) {
      log.info(
          "Rule for ID {} was not found. Ignoring rule for this transaction. This should only happen close to an update to a rule.");
      return true;
    }
    return false;
  }

  private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState)
      throws Exception {
    Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);
    if (widestWindowRule != null && widestWindowRule.getRuleState() == Rule.RuleState.ACTIVE) {
      if (widestWindowRule == null || widestWindowRule.getWindowMillis() < rule.getWindowMillis()) {
        broadcastState.put(WIDEST_RULE_KEY, rule);
      }
    }
  }

  public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out)
      throws Exception {

    Rule widestWindowRule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY);

    Optional<Long> cleanupEventTimeWindow =
        Optional.ofNullable(widestWindowRule).map(Rule::getWindowMillis);
    Optional<Long> cleanupEventTimeThreshold =
        cleanupEventTimeWindow.map(window -> timestamp - window);

    // TODO: add maximum allowed widest window instead (force cleanup, even if widest rule is
    // inactive)?
    cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow);
  }

  private void evictAgedElementsFromWindow(Long threshold) {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        Long stateEventTime = keys.next();
        if (stateEventTime < threshold) {
          keys.remove();
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void evictAllStateElements() {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        keys.next();
        keys.remove();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
