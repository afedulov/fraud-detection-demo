package com.ververica.field.dynamicrules.sources;

import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Main.Config;
import com.ververica.field.dynamicrules.Main.Params;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.functions.RuleDeserializer;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

public class RulesSource {

  private static final int RULES_STREAM_PARALLELISM = 1;

  public static SourceFunction<String> createRulesSource(ParameterTool params, Config config)
      throws IOException {

    RulesSource.Type rulesSourceType = getRulesSourceTypeFromParams(params);

    switch (rulesSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(params);
        String rulesTopic = params.get(Params.DATA_TOPIC_PARAM, Params.DEFAULT_RULES_TOPIC);
        FlinkKafkaConsumer011<String> kafkaConsumer =
            new FlinkKafkaConsumer011<>(rulesTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
      case PUBSUB:
        return PubSubSource.<String>newBuilder()
            .withDeserializationSchema(new SimpleStringSchema())
            .withProjectName(config.GCP_PROJECT_NAME)
            .withSubscriptionName(config.GCP_PUBSUB_RULES_SUBSCRIPTION_NAME)
            .build();
      case SOCKET:
        return new SocketTextStreamFunction("localhost", config.SOCKET_PORT, "\n", -1);
      default:
        throw new IllegalArgumentException(
            "Source \"" + rulesSourceType + "\" unknown. Known values are:" + Type.values());
    }
  }

  private static RulesSource.Type getRulesSourceTypeFromParams(ParameterTool params) {
    String sourceTypeString = params.get(Params.RULES_SOURCE_PARAM, Type.SOCKET.toString());

    return RulesSource.Type.valueOf(sourceTypeString.toUpperCase());
  }

  public static DataStream<Rule> stringsStreamToRules(DataStream<String> ruleStrings) {
    return ruleStrings
        .flatMap(new RuleDeserializer())
        .name("Rule Deserialization")
        .setParallelism(RULES_STREAM_PARALLELISM)
        .assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<Rule>(Time.of(0, TimeUnit.MILLISECONDS)) {
              @Override
              public long extractTimestamp(Rule element) {
                // Prevents connected data+update stream watermark stalling.
                return Long.MAX_VALUE;
              }
            });
  }

  public enum Type {
    KAFKA("Rules Source (Kafka)"),
    PUBSUB("Rules Source (Pub/Sub)"),
    SOCKET("Rules Source (Socket)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
