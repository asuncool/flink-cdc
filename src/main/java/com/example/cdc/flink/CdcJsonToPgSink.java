package com.example.cdc.flink;

import com.example.cdc.config.CdcProperties;
import com.example.cdc.sink.PostgresUpsertSink;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@Slf4j
@RequiredArgsConstructor
public class CdcJsonToPgSink extends RichSinkFunction<String> {

  private final CdcProperties props;

  private transient ObjectMapper objectMapper;
  private transient PostgresUpsertSink sink;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.objectMapper = new ObjectMapper();
    this.sink = new PostgresUpsertSink(props);
  }

  @Override
  public void invoke(String value, Context context) throws Exception {
    try {
      JsonNode root = objectMapper.readTree(value);
      sink.invoke(root);
    } catch (Exception e) {
      log.error("Failed to process CDC JSON: {}", value, e);
      throw e;
    }
  }
}
