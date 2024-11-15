package org.apache.flink.func;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.dto.PageView;

public class PostCodeCountAggFunc implements AggregateFunction<PageView, Long, Long> {
  @Override
  public Long createAccumulator() {
    return 0L;
  }

  @Override
  public Long add(PageView pageView, Long accumulator) {
    return accumulator + 1;
  }

  @Override
  public Long getResult(Long accumulator) {
    return accumulator;
  }

  @Override
  public Long merge(Long a, Long b) {
    return a + b;
  }
}
