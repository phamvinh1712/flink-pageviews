package org.apache.flink;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.dto.PageView;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;

public class KafkaUtils {
  private static final String BOOTSTRAP_SERVER = "localhost:9092";
  private static final String RAW_EVENTS_TOPIC = "pageviews";
  private static final String LATE_EVENTS_TOPIC = "pageviews-late-events";

  public static KafkaSource<String> buildKafkaSourceRaw(final String kafkaBootstrapServer) {
    final Properties kafkaProperties = buildKafkaProperties();

    return KafkaSource.<String>builder()
        .setBootstrapServers(kafkaBootstrapServer)
        .setProperties(kafkaProperties)
        .setTopics(RAW_EVENTS_TOPIC)
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setClientIdPrefix("raw-sink")
        .setGroupId("raw-sink")
        .build();
  }

  public static KafkaSource<PageView> buildKafkaSourcePageview(final String kafkaBootstrapServer) {
    final Properties kafkaProperties = buildKafkaProperties();
    JsonDeserializationSchema<PageView> jsonFormat =
        new JsonDeserializationSchema<>(PageView.class);

    return KafkaSource.<PageView>builder()
        .setBootstrapServers(kafkaBootstrapServer)
        .setProperties(kafkaProperties)
        .setTopics(RAW_EVENTS_TOPIC)
        .setValueOnlyDeserializer(jsonFormat)
        .setClientIdPrefix("aggregate")
        .setGroupId("aggregate")
        .build();
  }

  public static Properties buildKafkaProperties() {
    final Properties kafkaProperties = new Properties();
    kafkaProperties.setProperty("security.protocol", "PLAINTEXT");
    return kafkaProperties;
  }

  public static KafkaSink<PageView> buildKafkaSinkLateEvents(final String kafkaBootstrapServer) {
    return buildKafkaSink(kafkaBootstrapServer, LATE_EVENTS_TOPIC);
  }

  public static KafkaSink<PageView> buildKafkaSinkRawEvents(final String kafkaBootstrapServer) {
    return buildKafkaSink(kafkaBootstrapServer, RAW_EVENTS_TOPIC);
  }

  private static KafkaSink<PageView> buildKafkaSink(
      final String kafkaBootstrapServer, final String topic) {
    final Properties kafkaProperties = buildKafkaProperties();
    JsonSerializationSchema<PageView> jsonFormat = new JsonSerializationSchema<>();

    return KafkaSink.<PageView>builder()
        .setBootstrapServers(kafkaBootstrapServer)
        .setKafkaProducerConfig(kafkaProperties)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<PageView>builder()
                .setTopic(topic)
                .setValueSerializationSchema(jsonFormat)
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }
}
