package com.ververica.field.dynamicrules.sinks;

import com.ververica.field.dynamicrules.sinks.BaseSink.SinkType;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class BaseSinkConfig {

  private final String kafkaTopicParam;
  private final String kafkaDefaultTopic;

  private final String sinkTypeParam;
  private final String defaultSinkType = SinkType.STDOUT.toString();

  private final String gcpPubSubProjectName;
  private final String gcpPubSupTopicName;
}
