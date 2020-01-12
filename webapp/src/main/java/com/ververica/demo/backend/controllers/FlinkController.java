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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.demo.backend.entities.Rule;
import com.ververica.demo.backend.model.RulePayload;
import com.ververica.demo.backend.model.RulePayload.ControlType;
import com.ververica.demo.backend.model.RulePayload.RuleState;
import com.ververica.demo.backend.services.FlinkRulesService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class FlinkController {

  private final FlinkRulesService flinkRulesService;

  // Currently rules channel is also (mis)used for control messages. This has to do with how control
  // channels are set up in Flink Job.
  FlinkController(FlinkRulesService flinkRulesService) {
    this.flinkRulesService = flinkRulesService;
  }

  private final ObjectMapper mapper = new ObjectMapper();

  @GetMapping("/syncRules")
  void syncRules() throws JsonProcessingException {
    Rule command = createControllCommand(ControlType.EXPORT_RULES_CURRENT);
    flinkRulesService.addRule(command);
  }

  @GetMapping("/clearState")
  void clearState() throws JsonProcessingException {
    Rule command = createControllCommand(ControlType.CLEAR_STATE_ALL);
    flinkRulesService.addRule(command);
  }

  private Rule createControllCommand(ControlType clearStateAll) throws JsonProcessingException {
    RulePayload payload = new RulePayload();
    payload.setRuleState(RuleState.CONTROL);
    payload.setControlType(clearStateAll);
    Rule rule = new Rule();
    rule.setRulePayload(mapper.writeValueAsString(payload));
    return rule;
  }
}
