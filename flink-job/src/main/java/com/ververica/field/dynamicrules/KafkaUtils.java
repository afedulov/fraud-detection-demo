package com.ververica.field.dynamicrules;

import static com.ververica.field.config.Parameters.KAFKA_HOST;
import static com.ververica.field.config.Parameters.KAFKA_PORT;
import static com.ververica.field.config.Parameters.OFFSET;

import com.ververica.field.config.Config;
import java.util.Properties;

public class KafkaUtils {

  public static Properties initConsumerProperties(Config config) {
    Properties kafkaProps = initProperties(config);
    String offset = config.get(OFFSET);
    kafkaProps.setProperty("auto.offset.reset", offset);
    return kafkaProps;
  }

  public static Properties initProducerProperties(Config params) {
    return initProperties(params);
  }

  private static Properties initProperties(Config config) {
    Properties kafkaProps = new Properties();
    String kafkaHost = config.get(KAFKA_HOST);
    int kafkaPort = config.get(KAFKA_PORT);
    String servers = String.format("%s:%s", kafkaHost, kafkaPort);
    kafkaProps.setProperty("bootstrap.servers", servers);
    return kafkaProps;
  }
}
