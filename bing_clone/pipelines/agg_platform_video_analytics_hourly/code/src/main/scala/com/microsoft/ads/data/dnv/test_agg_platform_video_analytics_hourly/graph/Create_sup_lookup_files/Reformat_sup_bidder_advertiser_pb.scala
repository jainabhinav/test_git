package com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.UDFs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_sup_bidder_advertiser_pb {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("id").cast(IntegerType).as("id"),
      col("member_id").cast(IntegerType).as("member_id"),
      col("advertiser_default_currency"),
      col("is_running_political_ads"),
      col("name")
    )

}
