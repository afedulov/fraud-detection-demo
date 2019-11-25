package com.ververica.field.dynamicrules.sinks;

import static com.ververica.field.config.Parameters.GCP_PROJECT_NAME;
import static com.ververica.field.config.Parameters.GCP_PUBSUB_LATENCY_SUBSCRIPTION;
import static com.ververica.field.config.Parameters.LATENCY_SINK;
import static com.ververica.field.config.Parameters.LATENCY_TOPIC;

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.KafkaUtils;
import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class LatencySink {

  public static SinkFunction<String> createLatencySink(Config config)
      throws IOException {

    String latencySink = config.get(LATENCY_SINK);
    LatencySink.Type latencySinkType = LatencySink.Type.valueOf(latencySink.toUpperCase());

    switch (latencySinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(config);
        String latencyTopic = config.get(LATENCY_TOPIC);
        return new FlinkKafkaProducer011<>(latencyTopic, new SimpleStringSchema(), kafkaProps);
      case PUBSUB:
        return PubSubSink.<String>newBuilder()
            .withSerializationSchema(new SimpleStringSchema())
            .withProjectName(config.get(GCP_PROJECT_NAME))
            .withTopicName(config.get(GCP_PUBSUB_LATENCY_SUBSCRIPTION))
            .build();
      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + latencySinkType + "\" unknown. Known values are:" + Type.values());
    }
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
