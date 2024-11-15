package org.apache.flink;

import static org.apache.flink.IcebergUtils.PAGE_VIEW_TABLE_SCHEMA;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.compactor.ConcatFileCompactor;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.func.PageViewRawRowMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public class RawSinkJob {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //    env.getConfig().getSerializerConfig().setGenericTypes(false);
    ParameterTool parameters = ParameterTool.fromArgs(args);
    final String kafkaBootstrapServer = parameters.getRequired(Params.KAFKA_BOOTSTRAP_SERVER);
    final String icebergWarehousePath = parameters.getRequired(Params.ICEBERG_WAREHOUSE_PATH);
    final String rawSinkPath = parameters.getRequired(Params.RAW_SINK_PATH);
    KafkaSource<String> source = KafkaUtils.buildKafkaSourceRaw(kafkaBootstrapServer);

    DataStream<String> rawEvents =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .uid("KafkaSource")
            .setParallelism(1);
    DataStream<Row> rowPageViews =
        rawEvents
            .map(new PageViewRawRowMapper())
            .uid("PageViewRawRowMap")
            .returns(
                ExternalTypeInfo.of(
                    FlinkSchemaUtil.toSchema(PAGE_VIEW_TABLE_SCHEMA).toRowDataType()));

    CatalogLoader catalogLoader = IcebergUtils.buildCatalogLoader(icebergWarehousePath);
    Catalog catalog = catalogLoader.loadCatalog();

    if (!catalog.tableExists(IcebergUtils.PAGE_VIEW_TABLE)) {
      catalog.createTable(
          IcebergUtils.PAGE_VIEW_TABLE,
          PAGE_VIEW_TABLE_SCHEMA,
          PartitionSpec.builderFor(PAGE_VIEW_TABLE_SCHEMA).day("timestamp").build());
    }
    FlinkSink.forRow(rowPageViews, FlinkSchemaUtil.toSchema(PAGE_VIEW_TABLE_SCHEMA))
        .uidPrefix("IcebergSinkPageView")
        .tableLoader(TableLoader.fromCatalog(catalogLoader, IcebergUtils.PAGE_VIEW_TABLE))
        .writeParallelism(2)
        .append();

    final FileSink<String> sink =
        FileSink.forRowFormat(new Path(rawSinkPath), new SimpleStringEncoder<String>("UTF-8"))
            .withBucketAssigner(new DateTimeBucketAssigner<>("'year='yyyy'/month='MM'/day='dd"))
            .withOutputFileConfig(new OutputFileConfig("raw-pageviews", ".jsonl"))
            .enableCompact(
                FileCompactStrategy.Builder.newBuilder()
                    .setSizeThreshold(1024)
                    .enableCompactionOnCheckpoint(5)
                    .build(),
                new ConcatFileCompactor())
            .build();
    rawEvents.sinkTo(sink).uid("JsonSink");

    env.execute("Raw Sink");
  }
}
