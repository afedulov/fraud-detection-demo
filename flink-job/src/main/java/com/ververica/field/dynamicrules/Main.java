package com.ververica.field.dynamicrules;

import java.io.Serializable;

public class Main {

  public static void main(String[] args) throws Exception {
    RulesEvaluator rulesEvaluator = new RulesEvaluator(new Config());
    rulesEvaluator.run(args);
  }

  public static class Config implements Serializable {
    public final int CHECKPOINT_INTERVAL = 60_000_0;
    public final int MIN_PAUSE_BETWEEN_CHECKPOINTS = 60_000_0;
    public final int OUT_OF_ORDERNESS = 500;

    public final String GCP_PROJECT_NAME = "da-fe-212612";
    public final String GCP_PUBSUB_RULES_SUBSCRIPTION_NAME = "rules-demo";
    public final String GCP_PUBSUB_ALERTS_TOPIC_NAME = "alerts-demo";
    public final String GCP_PUBSUB_LATENCY_TOPIC_NAME = "latency-demo";
    public final String GCP_PUBSUB_RULES_EXPORT_TOPIC_NAME = "current-rules-demo";

    public final int SOCKET_PORT = 9999;
  }

  public static class Params implements Serializable {
    // Param names
    public static final String KAFKA_HOST_PARAM = "kafka-host";
    public static final String KAFKA_PORT_PARAM = "kafka-port";

    public static final String DATA_TOPIC_PARAM = "data-topic";
    public static final String ALERTS_TOPIC_PARAM = "alerts-topic";
    public static final String LATENCY_TOPIC_PARAM = "latency-topic";
    public static final String RULES_EXPORT_TOPIC_PARAM = "current-rules-topic";

    public static final String OFFSET_PARAM = "offset";
    public static final String RECORDS_PER_SECOND_PARAM = "recordsPerSecond";
    public static final String RULES_SOURCE_PARAM = "rules-source";
    public static final String TRANSACTIONS_SOURCE_PARAM = "data-source";

    public static final String ALERTS_SINK_PARAM = "alerts-sink";
    public static final String LATENCY_SINK_PARAM = "latency-sink";
    public static final String RULES_EXPORT_SINK_PARAM = "rules-export-sink";

    public static final String LOCAL_EXECUTION_PARAM = "local";

    // Defaults
    // TODO: move to config.
    public static final int DEFAULT_TRANSACTIONS_PER_SECOND = 2;
    public static final int DEFAULT_SOURCE_PARALLELISM = 2;

    public static final String DEFAULT_KAFKA_HOST = "localhost";
    public static final int DEFAULT_KAFKA_PORT = 9092;

    public static final String DEFAULT_TRANSACTIONS_TOPIC = "livetransactions";
    public static final String DEFAULT_RULES_TOPIC = "rules";
    public static final String DEFAULT_ALERTS_TOPIC = "alerts";
    public static final String DEFAULT_LATENCY_TOPIC = "latency";
    public static final String DEFAULT_RULES_EXPORT_TOPIC = "current-rules";

    public static final String DEFAULT_TOPIC_OFFSET = "latest";
  }
}
