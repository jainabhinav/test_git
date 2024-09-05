package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SchemaTransform_DefaultToNull_agg_impbus_clicks {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.withColumn("clear_fees",
                  when(col("clear_fees") === lit(0.0d),
                       lit(null).cast(FloatType)
                  ).otherwise(col("clear_fees"))
      )
      .withColumn("inv_code",
                  when(col("inv_code") === lit("---"),
                       lit(null).cast(StringType)
                  ).otherwise(col("inv_code"))
      )
      .withColumn("application_id",
                  when(col("application_id") === lit("---"),
                       lit(null).cast(StringType)
                  ).otherwise(col("application_id"))
      )
      .withColumn("vp_expose_age",
                  when(col("vp_expose_age") === lit(0),
                       lit(null).cast(IntegerType)
                  ).otherwise(col("vp_expose_age"))
      )
      .withColumn("vp_expose_gender",
                  when(col("vp_expose_gender") === lit(0),
                       lit(null).cast(IntegerType)
                  ).otherwise(col("vp_expose_gender"))
      )
      .withColumn("sdk_version",
                  when(col("sdk_version") === lit("---"),
                       lit(null).cast(StringType)
                  ).otherwise(col("sdk_version"))
      )
      .withColumn("postal",
                  when(col("postal") === lit("---"), lit(null).cast(StringType))
                    .otherwise(col("postal"))
      )
      .withColumn("external_uid",
                  when(col("external_uid") === lit("---"),
                       lit(null).cast(StringType)
                  ).otherwise(col("external_uid"))
      )
      .withColumn("traffic_source_code",
                  when(col("traffic_source_code") === lit("---"),
                       lit(null).cast(StringType)
                  ).otherwise(col("traffic_source_code"))
      )
      .withColumn("external_request_id",
                  when(col("external_request_id") === lit("---"),
                       lit(null).cast(StringType)
                  ).otherwise(col("external_request_id"))
      )
      .withColumn("stitch_group_id",
                  when(col("stitch_group_id") === lit("---"),
                       lit(null).cast(StringType)
                  ).otherwise(col("stitch_group_id"))
      )
      .withColumn("counterparty_ruleset_type",
                  when(col("counterparty_ruleset_type") === lit(0),
                       lit(null).cast(IntegerType)
                  ).otherwise(col("counterparty_ruleset_type"))
      )
      .withColumn("site_domain",
                  when(col("site_domain") === lit("---"),
                       lit(null).cast(StringType)
                  ).otherwise(col("site_domain"))
      )

}
