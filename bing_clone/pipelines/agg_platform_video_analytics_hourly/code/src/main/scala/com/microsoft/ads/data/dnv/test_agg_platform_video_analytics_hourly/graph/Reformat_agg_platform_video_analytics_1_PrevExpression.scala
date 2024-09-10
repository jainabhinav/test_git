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

object Reformat_agg_platform_video_analytics_1_PrevExpression {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.withColumn(
      "in_f_get_agg_platform_video_analytics_hourly_pb_currency",
      f_get_agg_platform_video_analytics_hourly_pb_currency(
        col("fx_rate_snapshot_id").cast(IntegerType),
        col("advertiser_currency"),
        col("advertiser_exchange_rate"),
        col("publisher_currency"),
        col("publisher_exchange_rate"),
        col("advertiser_id").cast(IntegerType),
        col("imp_type").cast(IntegerType),
        col("buyer_member_id").cast(IntegerType),
        col("seller_member_id").cast(IntegerType),
        col("_sup_bidder_advertiser_pb_LOOKUP")
      )
    )

}
