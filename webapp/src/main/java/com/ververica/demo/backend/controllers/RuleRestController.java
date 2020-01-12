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
import com.ververica.demo.backend.exceptions.RuleNotFoundException;
import com.ververica.demo.backend.model.RulePayload;
import com.ververica.demo.backend.repositories.RuleRepository;
import com.ververica.demo.backend.services.FlinkRulesService;
import java.io.IOException;
import java.util.List;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
class RuleRestController {

  private final RuleRepository repository;
  private final FlinkRulesService flinkRulesService;

  RuleRestController(RuleRepository repository, FlinkRulesService flinkRulesService) {
    this.repository = repository;
    this.flinkRulesService = flinkRulesService;
  }

  private final ObjectMapper mapper = new ObjectMapper();

  @GetMapping("/rules")
  List<Rule> all() {
    return repository.findAll();
  }

  @PostMapping("/rules")
  Rule newRule(@RequestBody Rule newRule) throws IOException {
    Rule savedRule = repository.save(newRule);
    Integer id = savedRule.getId();
    RulePayload payload = mapper.readValue(savedRule.getRulePayload(), RulePayload.class);
    payload.setRuleId(id);
    String payloadJson = mapper.writeValueAsString(payload);
    savedRule.setRulePayload(payloadJson);
    Rule result = repository.save(savedRule);
    flinkRulesService.addRule(result);
    return result;
  }

  @GetMapping("/rules/pushToFlink")
  void pushToFlink() {
    List<Rule> rules = repository.findAll();
    for (Rule rule : rules) {
      flinkRulesService.addRule(rule);
    }
  }

  @GetMapping("/rules/{id}")
  Rule one(@PathVariable Integer id) {
    return repository.findById(id).orElseThrow(() -> new RuleNotFoundException(id));
  }

  @DeleteMapping("/rules/{id}")
  void deleteRule(@PathVariable Integer id) throws JsonProcessingException {
    repository.deleteById(id);
    flinkRulesService.deleteRule(id);
  }

  @DeleteMapping("/rules")
  void deleteAllRules() throws JsonProcessingException {
    List<Rule> rules = repository.findAll();
    for (Rule rule : rules) {
      repository.deleteById(rule.getId());
      flinkRulesService.deleteRule(rule.getId());
    }
  }
}
