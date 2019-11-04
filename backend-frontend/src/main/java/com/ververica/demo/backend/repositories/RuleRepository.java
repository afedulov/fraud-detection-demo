package com.ververica.demo.backend.repositories;

import com.ververica.demo.backend.entities.Rule;
import java.util.List;
import org.springframework.data.repository.CrudRepository;

public interface RuleRepository extends CrudRepository<Rule, Integer> {

  @Override
  List<Rule> findAll();
}
