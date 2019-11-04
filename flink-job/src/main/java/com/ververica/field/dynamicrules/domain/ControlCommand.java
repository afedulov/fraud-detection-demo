package com.ververica.field.dynamicrules.domain;

import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ControlCommand {

  private CommandType command;

  enum CommandType {
    CLEAR_STATE,
    PRINT_RULES;
  }

  public static void main(String[] args) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ControlCommand command = new ControlCommand(CommandType.CLEAR_STATE);
    String ser = mapper.writeValueAsString(command);
    System.out.println(ser);
    ControlCommand de = mapper.readValue(ser, ControlCommand.class);
    System.out.println(de);
  }
}
