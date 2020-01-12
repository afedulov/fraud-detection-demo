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

package com.ververica.demo.backend.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaTransactionsConsumerService implements ConsumerSeekAware {

  private final SimpMessagingTemplate simpTemplate;
  private final ObjectMapper mapper = new ObjectMapper();

  @Value("${web-socket.topic.transactions}")
  private String transactionsWebSocketTopic;

  @Autowired
  public KafkaTransactionsConsumerService(SimpMessagingTemplate simpTemplate) {
    this.simpTemplate = simpTemplate;
  }

  @KafkaListener(
      id = "${kafka.listeners.transactions.id}",
      topics = "${kafka.topic.transactions}",
      groupId = "transactions")
  public void consumeTransactions(@Payload String message) {
    log.debug("{}", message);
    simpTemplate.convertAndSend(transactionsWebSocketTopic, message);
  }

  @Override
  public void registerSeekCallback(ConsumerSeekCallback callback) {}

  @Override
  public void onPartitionsAssigned(
      Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    assignments.forEach((t, o) -> callback.seekToEnd(t.topic(), t.partition()));
  }

  @Override
  public void onIdleContainer(
      Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {}
}
