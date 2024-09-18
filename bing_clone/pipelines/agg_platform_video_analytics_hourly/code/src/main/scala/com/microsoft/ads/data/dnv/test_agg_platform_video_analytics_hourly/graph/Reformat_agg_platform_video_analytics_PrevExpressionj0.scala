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

object Reformat_agg_platform_video_analytics_PrevExpressionj0 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1").hint("broadcast"),
            col("in0.advertiser_id").cast(IntegerType) === col("in1.id"),
            "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.id")),
          struct(
            col("in1.id").as("id"),
            col("in1.member_id").as("member_id"),
            col("in1.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in1.is_running_political_ads").as("is_running_political_ads"),
            col("in1.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP"),
        col("in0.*")
      )

}
