package org.apache.flink.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PostCodeViewCount {
  private String postcode;
  private Long timestamp;
  private Long count;

  public PostCodeViewCount() {}

  public PostCodeViewCount(String postcode, Long count, Long timestamp) {
    this.postcode = postcode;
    this.timestamp = timestamp;
    this.count = count;
  }
}
