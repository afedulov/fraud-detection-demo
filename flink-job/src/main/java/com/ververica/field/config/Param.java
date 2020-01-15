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

import lombok.Getter;

@Getter
public class Param<T> {

  private String name;
  private Class<T> type;
  private T defaultValue;

  Param(String name, T defaultValue, Class<T> type) {
    this.name = name;
    this.type = type;
    this.defaultValue = defaultValue;
  }

  public static Param<String> string(String name, String defaultValue) {
    return new Param<>(name, defaultValue, String.class);
  }

  public static Param<Integer> integer(String name, Integer defaultValue) {
    return new Param<>(name, defaultValue, Integer.class);
  }

  public static Param<Boolean> bool(String name, Boolean defaultValue) {
    return new Param<>(name, defaultValue, Boolean.class);
  }
}
