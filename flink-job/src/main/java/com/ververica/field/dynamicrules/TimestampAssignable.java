package com.ververica.field.dynamicrules;

public interface TimestampAssignable<T> {
  void assignIngestionTimestamp(T timestamp);
}
