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

import java.lang.reflect.Field;
import java.math.BigDecimal;

public class FieldsExtractor {

  public static String getFieldAsString(Object object, String fieldName)
      throws IllegalAccessException, NoSuchFieldException {
    Class cls = object.getClass();
    Field field = cls.getField(fieldName);
    return field.get(object).toString();
  }

  public static double getDoubleByName(String fieldName, Object object)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = object.getClass().getField(fieldName);
    return (double) field.get(object);
  }

  public static BigDecimal getBigDecimalByName(String fieldName, Object object)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = object.getClass().getField(fieldName);
    return new BigDecimal(field.get(object).toString());
  }

  @SuppressWarnings("unchecked")
  public static <T> T getByKeyAs(String keyName, Object object)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = object.getClass().getField(keyName);
    return (T) field.get(object);
  }
}
