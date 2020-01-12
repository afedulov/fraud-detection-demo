/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.demo.backend.controllers;

import com.ververica.demo.backend.datasource.DemoTransactionsGenerator;
import com.ververica.demo.backend.datasource.TransactionsGenerator;
import com.ververica.demo.backend.services.KafkaTransactionsPusher;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class DataGenerationController {

  private TransactionsGenerator transactionsGenerator;
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  private ExecutorService executor = Executors.newSingleThreadExecutor();
  private boolean generatingTransactions = false;
  private boolean listenerContainerRunning = true;

  @Value("${kafka.listeners.transactions.id}")
  private String transactionListenerId;

  @Value("${transactionsRateDisplayLimit}")
  private int transactionsRateDisplayLimit;

  @Autowired
  public DataGenerationController(
      KafkaTransactionsPusher transactionsPusher,
      KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
    transactionsGenerator = new DemoTransactionsGenerator(transactionsPusher, 1);
    this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
  }

  @GetMapping("/api/startTransactionsGeneration")
  public void startTransactionsGeneration() throws Exception {
    log.info("{}", "startTransactionsGeneration called");
    generateTransactions();
  }

  private void generateTransactions() {
    if (!generatingTransactions) {
      executor.submit(transactionsGenerator);
      generatingTransactions = true;
    }
  }

  @GetMapping("/api/stopTransactionsGeneration")
  public void stopTransactionsGeneration() {
    transactionsGenerator.cancel();
    generatingTransactions = false;
    log.info("{}", "stopTransactionsGeneration called");
  }

  @GetMapping("/api/generatorSpeed/{speed}")
  public void setGeneratorSpeed(@PathVariable Long speed) {
    log.info("Generator speed change request: " + speed);
    if (speed <= 0) {
      transactionsGenerator.cancel();
      generatingTransactions = false;
      return;
    } else {
      generateTransactions();
    }

    MessageListenerContainer listenerContainer =
        kafkaListenerEndpointRegistry.getListenerContainer(transactionListenerId);
    if (speed > transactionsRateDisplayLimit) {
      listenerContainer.stop();
      listenerContainerRunning = false;
    } else if (!listenerContainerRunning) {
      listenerContainer.start();
    }

    if (transactionsGenerator != null) {
      transactionsGenerator.adjustMaxRecordsPerSecond(speed);
    }
  }
}
