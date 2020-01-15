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

package com.ververica.field.dynamicrules.functions;

import com.ververica.field.sources.BaseGenerator;
import java.util.SplittableRandom;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonGeneratorWrapper<T> extends BaseGenerator<String> {

  private BaseGenerator<T> wrappedGenerator;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public JsonGeneratorWrapper(BaseGenerator<T> wrappedGenerator) {
    this.wrappedGenerator = wrappedGenerator;
    this.maxRecordsPerSecond = wrappedGenerator.getMaxRecordsPerSecond();
  }

  @Override
  public String randomEvent(SplittableRandom rnd, long id) {
    T transaction = wrappedGenerator.randomEvent(rnd, id);
    String json;
    try {
      json = objectMapper.writeValueAsString(transaction);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return json;
  }
}
