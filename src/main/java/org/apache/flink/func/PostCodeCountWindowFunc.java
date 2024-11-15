package org.apache.flink.func;

import org.apache.flink.dto.PostCodeViewCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PostCodeCountWindowFunc
    extends ProcessWindowFunction<Long, PostCodeViewCount, String, TimeWindow> {

  @Override
  public void process(
      String postCode,
      ProcessWindowFunction<Long, PostCodeViewCount, String, TimeWindow>.Context context,
      Iterable<Long> iterable,
      Collector<PostCodeViewCount> collector)
      throws Exception {
    final Long count = iterable.iterator().next();
    collector.collect(new PostCodeViewCount(postCode, count, context.window().getStart()));
  }
}
