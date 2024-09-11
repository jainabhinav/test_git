package io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Pre_Rollup_agg_platform_video_slot_analytics_pb {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("key")).agg(value(context).as("value"))

  def value(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      sum(
        when(
          is_not_null(col("value.reseller_revenue_dollars")).cast(BooleanType),
          col("value.reseller_revenue_dollars")
        )
      ).as("reseller_revenue_dollars"),
      sum(
        when(is_not_null(col("value.booked_revenue_dollars")).cast(BooleanType),
             col("value.booked_revenue_dollars")
        )
      ).as("booked_revenue_dollars"),
      sum(
        when(is_not_null(col("value.imps").cast(LongType)).cast(BooleanType),
             col("value.imps").cast(LongType)
        )
      ).as("imps"),
      sum(
        when(is_not_null(col("value.starts").cast(LongType)).cast(BooleanType),
             col("value.starts").cast(LongType)
        )
      ).as("starts"),
      sum(
        when(is_not_null(col("value.skips").cast(LongType)).cast(BooleanType),
             col("value.skips").cast(LongType)
        )
      ).as("skips"),
      sum(
        when(is_not_null(col("value.errors").cast(LongType)).cast(BooleanType),
             col("value.errors").cast(LongType)
        )
      ).as("errors"),
      sum(
        when(is_not_null(col("value.first_quartiles").cast(LongType))
               .cast(BooleanType),
             col("value.first_quartiles").cast(LongType)
        )
      ).as("first_quartiles"),
      sum(
        when(is_not_null(col("value.second_quartiles").cast(LongType))
               .cast(BooleanType),
             col("value.second_quartiles").cast(LongType)
        )
      ).as("second_quartiles"),
      sum(
        when(is_not_null(col("value.third_quartiles").cast(LongType))
               .cast(BooleanType),
             col("value.third_quartiles").cast(LongType)
        )
      ).as("third_quartiles"),
      sum(
        when(is_not_null(col("value.completions").cast(LongType))
               .cast(BooleanType),
             col("value.completions").cast(LongType)
        )
      ).as("completions"),
      sum(
        when(is_not_null(col("value.clicks").cast(LongType)).cast(BooleanType),
             col("value.clicks").cast(LongType)
        )
      ).as("clicks"),
      sum(
        when(is_not_null(col("value.ad_requests").cast(LongType))
               .cast(BooleanType),
             col("value.ad_requests").cast(LongType)
        )
      ).as("ad_requests"),
      sum(
        when(is_not_null(col("value.ad_responses").cast(LongType))
               .cast(BooleanType),
             col("value.ad_responses").cast(LongType)
        )
      ).as("ad_responses")
    )
  }

}
