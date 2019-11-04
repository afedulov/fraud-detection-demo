package com.ververica.field.dynamicrules.sinks;

import static com.ververica.field.dynamicrules.Main.Params.DEFAULT_LATENCY_TOPIC;
import static com.ververica.field.dynamicrules.Main.Params.LATENCY_SINK_PARAM;
import static com.ververica.field.dynamicrules.Main.Params.LATENCY_TOPIC_PARAM;

import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Main.Config;
import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class LatencySink {

  public static SinkFunction<String> createLatencySink(ParameterTool params, Config config)
      throws IOException {

    LatencySink.Type latencySinkType = getLatencySinkTypeFromParams(params);

    switch (latencySinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(params);
        String latencyTopic = params.get(LATENCY_TOPIC_PARAM, DEFAULT_LATENCY_TOPIC);
        return new FlinkKafkaProducer011<>(latencyTopic, new SimpleStringSchema(), kafkaProps);
      case PUBSUB:
        return PubSubSink.<String>newBuilder()
            .withSerializationSchema(new SimpleStringSchema())
            .withProjectName(config.GCP_PROJECT_NAME)
            .withTopicName(config.GCP_PUBSUB_LATENCY_TOPIC_NAME)
            .build();
      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + latencySinkType + "\" unknown. Known values are:" + Type.values());
    }
  }

  private static LatencySink.Type getLatencySinkTypeFromParams(ParameterTool params) {
    String sourceTypeString = params.get(LATENCY_SINK_PARAM, Type.STDOUT.toString());

    return LatencySink.Type.valueOf(sourceTypeString.toUpperCase());
  }

  public enum Type {
    KAFKA("Latency Sink (Kafka)"),
    PUBSUB("Latency Sink (Pub/Sub)"),
    STDOUT("Latency Sink (Std. Out)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
