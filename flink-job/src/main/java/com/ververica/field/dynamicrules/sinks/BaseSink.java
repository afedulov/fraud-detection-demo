package com.ververica.field.dynamicrules.sinks;

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

public class BaseSink {

  private final BaseSinkConfig sinkConfig;

  public BaseSink(BaseSinkConfig sinkConfig) {
    this.sinkConfig = sinkConfig;
  }

  public SinkFunction<String> createSink(ParameterTool params, Config config) throws IOException {

    SinkType sinkType = getSinkTypeFromParams(params);

    switch (sinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(params);
        String alertsTopic =
            params.get(sinkConfig.getKafkaTopicParam(), sinkConfig.getKafkaDefaultTopic());
        return new FlinkKafkaProducer011<>(alertsTopic, new SimpleStringSchema(), kafkaProps);
      case PUBSUB:
        return PubSubSink.<String>newBuilder()
            .withSerializationSchema(new SimpleStringSchema())
            .withProjectName(sinkConfig.getGcpPubSubProjectName())
            .withTopicName(sinkConfig.getGcpPubSupTopicName())
            .build();
      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + sinkType + "\" unknown. Known values are:" + AlertsSink.Type.values());
    }
  }

  private SinkType getSinkTypeFromParams(ParameterTool params) {
    String sourceTypeString =
        params.get(sinkConfig.getSinkTypeParam(), sinkConfig.getDefaultSinkType());

    return SinkType.valueOf(sourceTypeString.toUpperCase());
  }

  public enum SinkType {
    KAFKA("Sink (Kafka)"),
    PUBSUB("Sink (Pub/Sub)"),
    STDOUT("Sink (Std. Out)");

    private String name;

    SinkType(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
