package com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.UDFs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_agg_platform_video_analyticsj0 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.inventory_url_id").cast(IntegerType) === col(
              "in1.inventory_url_id"
            ),
            "left_outer"
      )
      .join(in2.as("in2"),
            col("in0.inventory_url_id").cast(IntegerType) === col(
              "in2.inventory_url_id"
            ),
            "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.inventory_url_id")),
          struct(
            col("in1.inventory_url_id").as("inventory_url_id"),
            col("in1.content_category_id").as("content_category_id"),
            col("in1.parent_category_id").as("parent_category_id")
          )
        ).as("_sup_api_inventory_url_content_category_LOOKUP"),
        when(
          is_not_null(col("in2.inventory_url_id")),
          struct(col("in2.inventory_url_id").as("inventory_url_id"),
                 col("in2.url").as("url"),
                 col("in2.list").as("list"),
                 col("in2.quality_index").as("quality_index")
          )
        ).as("_sup_inventory_url_pb_LOOKUP"),
        col("in0.*")
      )

}
