package org.apache.flink.func;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.dto.PageView;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class PageViewRawRowMapper implements MapFunction<String, Row> {
  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public Row map(String value) throws Exception {
    final PageView pageView = objectMapper.readValue(value, PageView.class);
    return Row.ofKind(
        RowKind.INSERT,
        pageView.getUserId(),
        pageView.getPostcode(),
        pageView.getWebpage(),
        Instant.ofEpochMilli(pageView.getTimestamp()));
  }
}
