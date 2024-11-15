package org.apache.flink;

import static org.apache.flink.IcebergUtils.POSTCODE_VIEW_COUNT_TABLE_SCHEMA;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.dto.PageView;
import org.apache.flink.dto.PostCodeViewCount;
import org.apache.flink.func.PostCodeCountAggFunc;
import org.apache.flink.func.PostCodeCountWindowFunc;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public class PostcodeAggregateJob {
  public static final OutputTag<PageView> LATE_DATA_OUTPUT = new OutputTag<>("LATE_DATA") {};

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //    env.getConfig().getSerializerConfig().setGenericTypes(false);
    ParameterTool parameters = ParameterTool.fromArgs(args);
    final String kafkaBootstrapServer = parameters.getRequired(Params.KAFKA_BOOTSTRAP_SERVER);
    final String icebergWarehousePath = parameters.getRequired(Params.ICEBERG_WAREHOUSE_PATH);

    KafkaSource<PageView> source = KafkaUtils.buildKafkaSourcePageview(kafkaBootstrapServer);
    SingleOutputStreamOperator<Row> result =
        env.fromSource(
                source,
                WatermarkStrategy.<PageView>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                    .withTimestampAssigner((event, timestamp) -> event.getTimestamp()),
                "Kafka Source")
            .uid("KafkaSource")
            .setParallelism(1)
            .keyBy(PageView::getPostcode)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(60)))
            .allowedLateness(Duration.ofDays(1))
            .sideOutputLateData(LATE_DATA_OUTPUT)
            .aggregate(new PostCodeCountAggFunc(), new PostCodeCountWindowFunc())
            .uid("PostCodeCountAgg")
            .map(
                (MapFunction<PostCodeViewCount, Row>)
                    value ->
                        Row.ofKind(
                            RowKind.INSERT,
                            value.getPostcode(),
                            Instant.ofEpochMilli(value.getTimestamp()),
                            value.getCount()))
            .returns(
                ExternalTypeInfo.of(
                    FlinkSchemaUtil.toSchema(POSTCODE_VIEW_COUNT_TABLE_SCHEMA).toRowDataType()));

    DataStream<PageView> lateStream = result.getSideOutput(LATE_DATA_OUTPUT);

    KafkaSink<PageView> lateEventsSink = KafkaUtils.buildKafkaSinkLateEvents(kafkaBootstrapServer);
    lateStream.sinkTo(lateEventsSink).uid("LateEventsSink");

    CatalogLoader catalogLoader = IcebergUtils.buildCatalogLoader(icebergWarehousePath);
    Catalog catalog = catalogLoader.loadCatalog();

    if (!catalog.tableExists(IcebergUtils.POSTCODE_VIEW_COUNT_TABLE)) {
      catalog.createTable(
          IcebergUtils.POSTCODE_VIEW_COUNT_TABLE,
          POSTCODE_VIEW_COUNT_TABLE_SCHEMA,
          PartitionSpec.unpartitioned());
    }
    FlinkSink.forRow(result, FlinkSchemaUtil.toSchema(POSTCODE_VIEW_COUNT_TABLE_SCHEMA))
        .uidPrefix("IcebergSinkPostCodeViewCount")
        .tableLoader(TableLoader.fromCatalog(catalogLoader, IcebergUtils.POSTCODE_VIEW_COUNT_TABLE))
        .equalityFieldColumns(Arrays.asList("postCode", "timestamp"))
        .upsert(true)
        .writeParallelism(2)
        .append();

    env.execute("PostCodeViewCount");
  }
}
