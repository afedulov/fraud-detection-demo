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

import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonMapper<T> {

  private final Class<T> targetClass;
  private final ObjectMapper objectMapper;

  public JsonMapper(Class<T> targetClass) {
    this.targetClass = targetClass;
    objectMapper = new ObjectMapper();
  }

  public T fromString(String line) throws IOException {
    return objectMapper.readValue(line, targetClass);
  }

  public String toString(T line) throws IOException {
    return objectMapper.writeValueAsString(line);
  }
}
