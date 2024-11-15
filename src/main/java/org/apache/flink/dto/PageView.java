package org.apache.flink.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PageView {

  private Integer userId;
  private String postcode;
  private String webpage;
  private Long timestamp;

  public PageView() {}

  public PageView(Integer userId, String postcode, String webpage, Long timestamp) {
    this.userId = userId;
    this.postcode = postcode;
    this.webpage = webpage;
    this.timestamp = timestamp;
  }
}
