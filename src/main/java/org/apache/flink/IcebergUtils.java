package org.apache.flink;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.types.Types;

public class IcebergUtils {
  public static final String DATABASE = "pageviews";
  public static final TableIdentifier POSTCODE_VIEW_COUNT_TABLE =
      TableIdentifier.of(DATABASE, "postcode_view_count");

  public static final TableIdentifier PAGE_VIEW_TABLE = TableIdentifier.of(DATABASE, "page_view");

  public static final Schema PAGE_VIEW_TABLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "userId", Types.IntegerType.get()),
          Types.NestedField.required(2, "postCode", Types.StringType.get()),
          Types.NestedField.required(3, "webpage", Types.StringType.get()),
          Types.NestedField.required(4, "timestamp", Types.TimestampType.withZone()));

  public static final Schema POSTCODE_VIEW_COUNT_TABLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "postCode", Types.StringType.get()),
          Types.NestedField.required(2, "timestamp", Types.TimestampType.withZone()),
          Types.NestedField.required(3, "count", Types.LongType.get()));

  public static CatalogLoader buildCatalogLoader(final String icebergWarehousePath) {
    Map<String, String> properties = new HashMap<>();

    properties.put("type", "iceberg");
    properties.put("catalog-type", "hadoop");
    properties.put("warehouse", icebergWarehousePath);
    properties.put("property-version", "1");

    return CatalogLoader.hadoop("iceberg", new org.apache.hadoop.conf.Configuration(), properties);
  }
}
