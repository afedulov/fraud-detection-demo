package com.ververica.field.dynamicrules.sources;

import static com.ververica.field.dynamicrules.Main.Params.*;

import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Transaction;
import com.ververica.field.dynamicrules.functions.JsonDeserializer;
import com.ververica.field.dynamicrules.functions.JsonGeneratorWrapper;
import com.ververica.field.dynamicrules.functions.TimeStamper;
import com.ververica.field.dynamicrules.functions.TransactionsGenerator;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

public class TransactionsSource {

  public static SourceFunction<String> createTransactionsSource(ParameterTool params) {

    TransactionsSource.Type transactionsSourceType = getTransactionsSourceTypeFromParams(params);

    int transactionsPerSecond =
        params.getInt(RECORDS_PER_SECOND_PARAM, DEFAULT_TRANSACTIONS_PER_SECOND);

    switch (transactionsSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(params);
        String transactionsTopic = params.get(DATA_TOPIC_PARAM, DEFAULT_TRANSACTIONS_TOPIC);
        FlinkKafkaConsumer011<String> kafkaConsumer =
            new FlinkKafkaConsumer011<>(transactionsTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
      default:
        return new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
    }
  }

  private static TransactionsSource.Type getTransactionsSourceTypeFromParams(ParameterTool params) {
    String sourceTypeString =
        params.get(TRANSACTIONS_SOURCE_PARAM, TransactionsSource.Type.GENERATOR.toString());
    return TransactionsSource.Type.valueOf(sourceTypeString.toUpperCase());
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
