package io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sup_api_member_pb_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "sup_api_member_pb",
      in,
      context.spark,
      List("id"),
      "id",
      "billing_name",
      "bidder_id",
      "is_billable",
      "enable_ip_truncation",
      "vendor_id",
      "final_auction_type_id",
      "default_referrer_url",
      "member_currency",
      "billing_currency"
    )

}
