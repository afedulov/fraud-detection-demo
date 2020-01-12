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

package com.ververica.demo.backend.datasource;

import java.math.BigDecimal;
import java.util.SplittableRandom;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DemoTransactionsGenerator extends TransactionsGenerator {

  private long lastPayeeIdBeneficiaryIdTriggered = System.currentTimeMillis();
  private long lastBeneficiaryIdTriggered = System.currentTimeMillis();
  private BigDecimal beneficiaryLimit = new BigDecimal(10000000);
  private BigDecimal payeeBeneficiaryLimit = new BigDecimal(20000000);

  public DemoTransactionsGenerator(Consumer<Transaction> consumer, int maxRecordsPerSecond) {
    super(consumer, maxRecordsPerSecond);
  }

  protected Transaction randomEvent(SplittableRandom rnd) {
    Transaction transaction = super.randomEvent(rnd);
    long now = System.currentTimeMillis();
    if (now - lastBeneficiaryIdTriggered > 8000 + rnd.nextInt(5000)) {
      transaction.setPaymentAmount(beneficiaryLimit.add(new BigDecimal(rnd.nextInt(1000000))));
      this.lastBeneficiaryIdTriggered = System.currentTimeMillis();
    }
    if (now - lastPayeeIdBeneficiaryIdTriggered > 12000 + rnd.nextInt(10000)) {
      transaction.setPaymentAmount(payeeBeneficiaryLimit.add(new BigDecimal(rnd.nextInt(1000000))));
      this.lastPayeeIdBeneficiaryIdTriggered = System.currentTimeMillis();
    }
    return transaction;
  }
}
