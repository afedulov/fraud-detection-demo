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

import com.ververica.demo.backend.entities.Rule;
import com.ververica.demo.backend.repositories.RuleRepository;
import com.ververica.demo.backend.services.FlinkRulesService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class RulesBootstrapper implements ApplicationRunner {

  private RuleRepository ruleRepository;
  private FlinkRulesService flinkRulesService;

  @Autowired
  public RulesBootstrapper(RuleRepository userRepository, FlinkRulesService flinkRulesService) {
    this.ruleRepository = userRepository;
    this.flinkRulesService = flinkRulesService;
  }

  public void run(ApplicationArguments args) {
    String payload1 =
        "{\"ruleId\":\"1\","
            + "\"aggregateFieldName\":\"paymentAmount\","
            + "\"aggregatorFunctionType\":\"SUM\","
            + "\"groupingKeyNames\":[\"payeeId\", \"beneficiaryId\"],"
            + "\"limit\":\"20000000\","
            + "\"limitOperatorType\":\"GREATER\","
            + "\"ruleState\":\"ACTIVE\","
            + "\"windowMinutes\":\"43200\"}";

    Rule rule1 = new Rule(payload1);

    String payload2 =
        "{\"ruleId\":\"2\","
            + "\"aggregateFieldName\":\"COUNT_FLINK\","
            + "\"aggregatorFunctionType\":\"SUM\","
            + "\"groupingKeyNames\":[\"paymentType\"],"
            + "\"limit\":\"300\","
            + "\"limitOperatorType\":\"LESS\","
            + "\"ruleState\":\"PAUSE\","
            + "\"windowMinutes\":\"1440\"}";

    Rule rule2 = new Rule(payload2);

    String payload3 =
        "{\"ruleId\":\"3\","
            + "\"aggregateFieldName\":\"paymentAmount\","
            + "\"aggregatorFunctionType\":\"SUM\","
            + "\"groupingKeyNames\":[\"beneficiaryId\"],"
            + "\"limit\":\"10000000\","
            + "\"limitOperatorType\":\"GREATER_EQUAL\","
            + "\"ruleState\":\"ACTIVE\","
            + "\"windowMinutes\":\"1440\"}";

    Rule rule3 = new Rule(payload3);

    String payload4 =
        "{\"ruleId\":\"4\","
            + "\"aggregateFieldName\":\"COUNT_WITH_RESET_FLINK\","
            + "\"aggregatorFunctionType\":\"SUM\","
            + "\"groupingKeyNames\":[\"paymentType\"],"
            + "\"limit\":\"100\","
            + "\"limitOperatorType\":\"GREATER_EQUAL\","
            + "\"ruleState\":\"ACTIVE\","
            + "\"windowMinutes\":\"1440\"}";

    Rule rule4 = new Rule(payload4);

    ruleRepository.save(rule1);
    ruleRepository.save(rule2);
    ruleRepository.save(rule3);
    ruleRepository.save(rule4);

    List<Rule> rules = ruleRepository.findAll();
    rules.forEach(rule -> flinkRulesService.addRule(rule));
  }
}
