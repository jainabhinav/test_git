package io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category_with_header_filter_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("inventory_url_id").cast(IntegerType).as("inventory_url_id"),
      col("content_category_id").cast(IntegerType).as("content_category_id"),
      when(string_compare(col("parent_category_id"), lit("NULL")) =!= lit(0),
           lpad(col("parent_category_id"), 8, " ")
      ).cast(IntegerType).as("parent_category_id")
    )

}
