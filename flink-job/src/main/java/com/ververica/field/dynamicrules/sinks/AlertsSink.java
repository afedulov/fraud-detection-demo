package com.ververica.field.dynamicrules.sinks;

import static com.ververica.field.dynamicrules.Main.Params.*;

import com.ververica.field.dynamicrules.Alert;
import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Main.Config;
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

public class AlertsSink {

  public static SinkFunction<String> createAlertsSink(ParameterTool params, Config config)
      throws IOException {

    AlertsSink.Type alertsSinkType = getAlertsSinkTypeFromParams(params);

    switch (alertsSinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(params);
        String alertsTopic = params.get(ALERTS_TOPIC_PARAM, DEFAULT_ALERTS_TOPIC);
        return new FlinkKafkaProducer011<>(alertsTopic, new SimpleStringSchema(), kafkaProps);
      case PUBSUB:
        return PubSubSink.<String>newBuilder()
            .withSerializationSchema(new SimpleStringSchema())
            .withProjectName(config.GCP_PROJECT_NAME)
            .withTopicName(config.GCP_PUBSUB_ALERTS_TOPIC_NAME)
            .build();
      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + alertsSinkType + "\" unknown. Known values are:" + Type.values());
    }
  }

  private static AlertsSink.Type getAlertsSinkTypeFromParams(ParameterTool params) {
    String sourceTypeString = params.get(ALERTS_SINK_PARAM, Type.STDOUT.toString());

    return AlertsSink.Type.valueOf(sourceTypeString.toUpperCase());
  }

  public static DataStream<String> alertsStreamToJson(DataStream<Alert> alerts) {
    return alerts.flatMap(new JsonSerializer<>(Alert.class)).name("Alerts Deserialization");
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
