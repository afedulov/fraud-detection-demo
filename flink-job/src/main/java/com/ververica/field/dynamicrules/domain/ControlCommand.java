package com.ververica.field.dynamicrules.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ControlCommand {

  private CommandType command;

  enum CommandType {
    CLEAR_STATE,
    PRINT_RULES;
  }
}
