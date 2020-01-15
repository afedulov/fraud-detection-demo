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
