/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransactionsSource {

  public static DataStreamSource<String> initTransactionsSource(
      Config config, StreamExecutionEnvironment env) {

    String sourceType = config.get(TRANSACTIONS_SOURCE);
    TransactionsSource.Type transactionsSourceType =
        TransactionsSource.Type.valueOf(sourceType.toUpperCase());
    int transactionsPerSecond = config.get(RECORDS_PER_SECOND);
    DataStreamSource<String> dataStreamSource;

    switch (transactionsSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String transactionsTopic = config.get(DATA_TOPIC);

        // NOTE: Idiomatically, watermarks should be assigned here, but this done later
        // because of the mix of the new Source (Kafka) and SourceFunction-based interfaces.
        // TODO: refactor when FLIP-238 is added

        KafkaSource<String> kafkaSource =
            KafkaSource.<String>builder()
                .setProperties(kafkaProps)
                .setTopics(transactionsTopic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        dataStreamSource =
            env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Rules Kafka Source");
        break;
      default:
        JsonGeneratorWrapper<Transaction> generatorSource =
            new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
        dataStreamSource = env.addSource(generatorSource);
    }
    return dataStreamSource;
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
