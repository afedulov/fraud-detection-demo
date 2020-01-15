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

package com.ververica.field.config;

import static com.ververica.field.config.Parameters.KAFKA_HOST;
import static com.ververica.field.config.Parameters.KAFKA_PORT;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ConfigTest {

  @Test
  public void testParameters() {
    String[] args = new String[] {"--kafka-host", "host-from-args"};
    Parameters parameters = Parameters.fromArgs(args);
    Config config = Config.fromParameters(parameters);

    final String kafkaHost = config.get(KAFKA_HOST);
    assertEquals("Wrong config parameter retrived", "host-from-args", kafkaHost);
  }

  @Test
  public void testParameterWithDefaults() {
    String[] args = new String[] {};
    Parameters parameters = Parameters.fromArgs(args);
    Config config = Config.fromParameters(parameters);

    final Integer kafkaPort = config.get(KAFKA_PORT);
    assertEquals("Wrong config parameter retrived", new Integer(9092), kafkaPort);
  }
}
