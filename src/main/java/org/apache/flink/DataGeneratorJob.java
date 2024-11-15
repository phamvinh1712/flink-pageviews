/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import net.datafaker.Faker;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.dto.PageView;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataGeneratorSource<PageView> source = buildDataGenSource();
    ParameterTool parameters = ParameterTool.fromArgs(args);
    final String kafkaBootstrapServer = parameters.getRequired(Params.KAFKA_BOOTSTRAP_SERVER);
    final KafkaSink<PageView> sink = KafkaUtils.buildKafkaSinkRawEvents(kafkaBootstrapServer);

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "Generator Source")
        .setParallelism(1)
        .sinkTo(sink);
    env.execute("Data Generator");
  }

  private static DataGeneratorSource<PageView> buildDataGenSource() {
    GeneratorFunction<Long, PageView> generatorFunction =
        index -> {
          final long randomSeed = index % 1000;
          final Random random = new Random(randomSeed);
          final int latePercentage = random.nextInt(100);
          Faker faker = new Faker(random);

          final Integer userId = faker.number().numberBetween(1, 10000);
          final String webpage = "www.website.com/index.html";
          final String postcode = faker.address().postcode();
          Long timestamp;

          // 80% of events will be late by at most 10 seconds
          if (latePercentage < 80) {
            timestamp = faker.date().past(10, TimeUnit.SECONDS).getTime();
          }
          // 10% of events will be late by at most 1 day
          else if (latePercentage < 90) {
            timestamp = faker.date().past(1, TimeUnit.DAYS).getTime();
          }
          // 10% of events will be late by at most 2 days
          else {
            timestamp = faker.date().past(2, TimeUnit.DAYS).getTime();
          }

          return new PageView(userId, postcode, webpage, timestamp);
        };

    return new DataGeneratorSource<>(
        generatorFunction,
        Long.MAX_VALUE,
        RateLimiterStrategy.perSecond(10),
        Types.POJO(PageView.class));
  }
}
