package com.ververica.field.dynamicrules;

import static com.ververica.field.dynamicrules.Main.Params.*;

import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;

public class KafkaUtils {

  public static Properties initConsumerProperties(ParameterTool params) {
    Properties kafkaProps = initProperties(params);
    String offset = params.get(OFFSET_PARAM, DEFAULT_TOPIC_OFFSET);
    kafkaProps.setProperty("auto.offset.reset", offset);
    return kafkaProps;
  }

  public static Properties initProducerProperties(ParameterTool params) {
    return initProperties(params);
  }

  private static Properties initProperties(ParameterTool params) {
    Properties kafkaProps = new Properties();
    String kafkaHost = params.get(KAFKA_HOST_PARAM, DEFAULT_KAFKA_HOST);
    int kafkaPort = params.getInt(KAFKA_PORT_PARAM, DEFAULT_KAFKA_PORT);
    String servers = String.format("%s:%s", kafkaHost, kafkaPort);
    kafkaProps.setProperty("bootstrap.servers", servers);
    return kafkaProps;
  }
}
