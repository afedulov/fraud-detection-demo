package com.ververica.field.dynamicrules;

import static com.ververica.field.dynamicrules.Main.Params.*;

import com.ververica.field.dynamicrules.Main.Config;
import com.ververica.field.dynamicrules.functions.AverageAggregate;
import com.ververica.field.dynamicrules.functions.DynamicKeyFunction;
import com.ververica.field.dynamicrules.functions.DynamicRuleFunction;
import com.ververica.field.dynamicrules.sinks.AlertsSink;
import com.ververica.field.dynamicrules.sinks.CurrentRulesSink;
import com.ververica.field.dynamicrules.sinks.LatencySink;
import com.ververica.field.dynamicrules.sources.RulesSource;
import com.ververica.field.dynamicrules.sources.TransactionsSource;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

@Slf4j
public class RulesEvaluator {

  // TODO: think about using ConfigOptions, like in JobManagerOptions
  private Config config;

  RulesEvaluator(Config config) {
    this.config = config;
  }

  public void run(String[] args) throws Exception {

    // Parameters
    ParameterTool params = ParameterTool.fromArgs(args);
    RulesSource.Type rulesSourceEnumType = getRulesSourceTypeFromParams(params);

    boolean isLocal = params.getBoolean(LOCAL_EXECUTION_PARAM, false);
    // Environment setup
    StreamExecutionEnvironment env =
        configureStreamExecutionEnvironment(rulesSourceEnumType, isLocal);
    //    ExecutionConfig executionConfig = env.getConfig();
    //    executionConfig.setParallelism(1);

    // Streams setup
    DataStream<Rule> rulesUpdateStream = getRulesUpdateStream(params, env);
    DataStream<Transaction> transactions = getTransactionsStream(params, env);

    // (Duplication is required because there is currently no way to cross-access broadcast state
    // between process functions)
    BroadcastStream<Rule> keysStream = rulesUpdateStream.broadcast(Descriptors.keysDescriptor);
    BroadcastStream<Rule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

    // Processing pipeline setup
    DataStream<Alert> alerts =
        transactions
            .connect(keysStream)
            .process(new DynamicKeyFunction())
            .name("Dynamic Partitioning Function")
            .keyBy((keyed) -> keyed.getKey())
            .connect(rulesStream)
            .process(new DynamicRuleFunction(config))
            .name("Dynamic Rule Evaluation Function");

    DataStream<String> allRuleEvaluations =
        ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.demoSinkTag);

    DataStream<Long> latency =
        ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.latencySinkTag);

    DataStream<Rule> currentRules =
        ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.currentRulesSinkTag);

    alerts.print().name("Alert STDOUT Sink");
    allRuleEvaluations.print().setParallelism(1).name("Rule Evaluation Sink");

    DataStream<String> alertsJson = AlertsSink.alertsStreamToJson(alerts);
    DataStream<String> currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules);

    currentRulesJson.print();

    alertsJson
        .addSink(AlertsSink.createAlertsSink(params, config))
        .setParallelism(1)
        .name("Alerts JSON Sink");
    currentRulesJson.addSink(CurrentRulesSink.createRulesSink(params, config)).setParallelism(1);

    // TODO: add DoubleSerializationSchema and switch sink type
    DataStream<String> latencies =
        latency.timeWindowAll(Time.seconds(10)).aggregate(new AverageAggregate());
    latencies.addSink(LatencySink.createLatencySink(params, config));

    env.execute();
  }

  private DataStream<Transaction> getTransactionsStream(
      ParameterTool params, StreamExecutionEnvironment env) {
    // Data stream setup
    SourceFunction<String> transactionSource = TransactionsSource.createTransactionsSource(params);
    int sourceParallelism = params.getInt("source-par", DEFAULT_SOURCE_PARALLELISM);
    DataStream<String> transactionsStringsStream =
        env.addSource(transactionSource)
            .name("Transactions Source")
            .setParallelism(sourceParallelism);
    DataStream<Transaction> transactionsStream =
        TransactionsSource.stringsStreamToTransactions(transactionsStringsStream);
    return transactionsStream.assignTimestampsAndWatermarks(
        createBoundedOutOfOrdernessTimestampExtractor(config.OUT_OF_ORDERNESS));
  }

  private DataStream<Rule> getRulesUpdateStream(
      ParameterTool params, StreamExecutionEnvironment env) throws IOException {

    RulesSource.Type rulesSourceEnumType = getRulesSourceTypeFromParams(params);

    SourceFunction<String> rulesSource = RulesSource.createRulesSource(params, config);
    DataStream<String> rulesStrings =
        env.addSource(rulesSource).name(rulesSourceEnumType.getName()).setParallelism(1);
    return RulesSource.stringsStreamToRules(rulesStrings);
  }

  private static RulesSource.Type getRulesSourceTypeFromParams(ParameterTool params) {
    String sourceTypeString = params.get(RULES_SOURCE_PARAM, RulesSource.Type.SOCKET.toString());
    return RulesSource.Type.valueOf(sourceTypeString.toUpperCase());
  }

  private StreamExecutionEnvironment configureStreamExecutionEnvironment(
      RulesSource.Type rulesSourceEnumType, boolean isLocal) {
    //    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration flinkConfig = new Configuration();
    flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

    StreamExecutionEnvironment env =
        isLocal
            ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
            : StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getCheckpointConfig().setCheckpointInterval(config.CHECKPOINT_INTERVAL);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.MIN_PAUSE_BETWEEN_CHECKPOINTS);

    configureRestartStrategy(env, rulesSourceEnumType);
    return env;
  }

  private AssignerWithPeriodicWatermarks<Transaction> createBoundedOutOfOrdernessTimestampExtractor(
      int outOfOrderness) {
    return new BoundedOutOfOrdernessTimestampExtractor<Transaction>(
        Time.of(outOfOrderness, TimeUnit.MILLISECONDS)) {
      @Override
      public long extractTimestamp(Transaction element) {
        return element.getEventTime();
      }
    };
  }

  private void configureRestartStrategy(
      StreamExecutionEnvironment env, RulesSource.Type rulesSourceEnumType) {
    switch (rulesSourceEnumType) {
      case SOCKET:
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(
                10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
        break;
      case KAFKA:
        // Default - unlimited restart strategy.
        //        env.setRestartStrategy(RestartStrategies.noRestart());
    }
  }

  public static class Descriptors {
    public static final MapStateDescriptor<Integer, Rule> keysDescriptor =
        new MapStateDescriptor<>(
            "keys", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));

    public static final MapStateDescriptor<Integer, Rule> rulesDescriptor =
        new MapStateDescriptor<>(
            "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));

    public static final OutputTag<String> demoSinkTag = new OutputTag<String>("demo-sink") {};
    public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {};
    public static final OutputTag<Rule> currentRulesSinkTag =
        new OutputTag<Rule>("current-rules-sink") {};
  }
}
