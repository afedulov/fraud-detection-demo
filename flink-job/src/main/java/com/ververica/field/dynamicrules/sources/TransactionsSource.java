package com.ververica.field.dynamicrules.sources;

import static com.ververica.field.config.Parameters.DATA_TOPIC;
import static com.ververica.field.config.Parameters.RECORDS_PER_SECOND;
import static com.ververica.field.config.Parameters.TRANSACTIONS_SOURCE;

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Transaction;
import com.ververica.field.dynamicrules.functions.JsonDeserializer;
import com.ververica.field.dynamicrules.functions.JsonGeneratorWrapper;
import com.ververica.field.dynamicrules.functions.TimeStamper;
import com.ververica.field.dynamicrules.functions.TransactionsGenerator;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

public class TransactionsSource {

  public static SourceFunction<String> createTransactionsSource(Config config) {

    String sourceType = config.get(TRANSACTIONS_SOURCE);
    TransactionsSource.Type transactionsSourceType =
        TransactionsSource.Type.valueOf(sourceType.toUpperCase());

    int transactionsPerSecond = config.get(RECORDS_PER_SECOND);

    switch (transactionsSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String transactionsTopic = config.get(DATA_TOPIC);
        FlinkKafkaConsumer011<String> kafkaConsumer =
            new FlinkKafkaConsumer011<>(transactionsTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
      default:
        return new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
    }
  }

  public static DataStream<Transaction> stringsStreamToTransactions(
      DataStream<String> transactionStrings) {
    return transactionStrings
        .flatMap(new JsonDeserializer<Transaction>(Transaction.class))
        .returns(Transaction.class)
        .flatMap(new TimeStamper<Transaction>())
        .returns(Transaction.class)
        .name("Transactions Deserialization");
  }

  public enum Type {
    GENERATOR("Transactions Source (generated locally)"),
    KAFKA("Transactions Source (Kafka)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
