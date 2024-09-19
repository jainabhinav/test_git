package io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_sup_api_member_pb {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("id").cast(IntegerType).as("id"),
      col("billing_name"),
      col("bidder_id").cast(IntegerType).as("bidder_id"),
      col("is_billable").cast(IntegerType).as("is_billable"),
      col("enable_ip_truncation").cast(IntegerType).as("enable_ip_truncation"),
      col("vendor_id").cast(IntegerType).as("vendor_id"),
      col("final_auction_type_id")
        .cast(IntegerType)
        .as("final_auction_type_id"),
      col("default_referrer_url"),
      col("member_currency"),
      col("billing_currency")
    )

}
