package com.ververica.field.dynamicrules.sinks;

import static com.ververica.field.dynamicrules.Main.Params.DEFAULT_RULES_EXPORT_TOPIC;
import static com.ververica.field.dynamicrules.Main.Params.RULES_EXPORT_SINK_PARAM;
import static com.ververica.field.dynamicrules.Main.Params.RULES_EXPORT_TOPIC_PARAM;

import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Main.Config;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.functions.JsonSerializer;
import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class CurrentRulesSink {

  public static SinkFunction<String> createRulesSink(ParameterTool params, Config config)
      throws IOException {

    CurrentRulesSink.Type currentRulesSinkType = getAlertsSinkTypeFromParams(params);

    switch (currentRulesSinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(params);
        String alertsTopic = params.get(RULES_EXPORT_TOPIC_PARAM, DEFAULT_RULES_EXPORT_TOPIC);
        return new FlinkKafkaProducer011<>(alertsTopic, new SimpleStringSchema(), kafkaProps);
      case PUBSUB:
        return PubSubSink.<String>newBuilder()
            .withSerializationSchema(new SimpleStringSchema())
            .withProjectName(config.GCP_PROJECT_NAME)
            .withTopicName(config.GCP_PUBSUB_RULES_EXPORT_TOPIC_NAME)
            .build();
      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + currentRulesSinkType + "\" unknown. Known values are:" + Type.values());
    }
  }

  private static CurrentRulesSink.Type getAlertsSinkTypeFromParams(ParameterTool params) {
    String sourceTypeString = params.get(RULES_EXPORT_SINK_PARAM, Type.STDOUT.toString());

    return CurrentRulesSink.Type.valueOf(sourceTypeString.toUpperCase());
  }

  public static DataStream<String> rulesStreamToJson(DataStream<Rule> alerts) {
    return alerts.flatMap(new JsonSerializer<>(Rule.class)).name("Rules Deserialization");
  }

  public enum Type {
    KAFKA("Alerts Sink (Kafka)"),
    PUBSUB("Alerts Sink (Pub/Sub)"),
    STDOUT("Alerts Sink (Std. Out)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
